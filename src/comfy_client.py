"""
comfy_client.py — Tương tác ComfyUI (WebSocket real-time + HTTP polling fallback).

Tối ưu latency end-to-end:
  • Thay polling /history mỗi 5s bằng WebSocket real-time (nhận event ngay lập tức).
  • Persistent WS connection + heartbeat để giữ alive qua RunPod Cloudflare proxy.
  • Tự động reconnect với exponential backoff.
  • Fallback tự động về polling cũ nếu WS không khả dụng (zero-downtime).

RunPod proxy format: https://{pod_id}-8188.proxy.runpod.net
Cloudflare 100s timeout → WS persistent tránh được giới hạn này.
"""
import asyncio
import time
from typing import Dict, Optional
from urllib.parse import urlencode

import httpx
import websockets
import websockets.exceptions
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings


# ─────────────────────────── WebSocket Client ───────────────────────────

class AsyncComfyWebSocketClient:
    """
    WebSocket client production-grade cho ComfyUI.

    Mỗi pod nên có 1 instance của class này (dùng chung nếu cần multiplex).
    Hỗ trợ:
      - Kết nối với retry (tenacity)
      - Heartbeat ping định kỳ để giữ alive qua Cloudflare/RunPod proxy
      - Listener loop bất đồng bộ nhận events real-time
      - Reconnect tự động với backoff
      - Nhiều prompt_id chờ đồng thời trên cùng 1 connection
    """

    def __init__(self, endpoint: str, client_id: str):
        # Chuyển http/https → ws/wss để dùng làm WebSocket URI
        self.endpoint = endpoint.rstrip("/")
        self.ws_base = self.endpoint.replace("https://", "wss://").replace("http://", "ws://")
        self.client_id = client_id

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # Mapping prompt_id → asyncio.Future để resolve khi nhận event
        self._listeners: Dict[str, asyncio.Future] = {}
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._is_closing = False
        self._connected = False

    # ── Connect ──────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Kết nối WebSocket tới ComfyUI. Retry tối đa COMFY_WS_RECONNECT_MAX_ATTEMPTS lần."""
        if self._connected and self._ws and not self._ws.closed:
            return

        uri = f"{self.ws_base}/ws?client_id={self.client_id}"
        # RunPod proxy yêu cầu Authorization header
        headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}

        @retry(
            stop=stop_after_attempt(settings.COMFY_WS_RECONNECT_MAX_ATTEMPTS),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True,
        )
        async def _try_connect():
            self._ws = await websockets.connect(
                uri,
                extra_headers=headers,
                # Ping tự động của websockets library (application-level ping_interval)
                ping_interval=settings.COMFY_WS_PING_INTERVAL,
                ping_timeout=30,
                close_timeout=10,
                # Tăng buffer để tránh drop message khi ComfyUI trả nhiều event liên tiếp
                max_size=10 * 1024 * 1024,  # 10 MB
            )

        await _try_connect()
        self._connected = True
        logger.info(
            f"[comfy-ws] ✅ Connected → {self.ws_base} (client_id={self.client_id})"
        )

        # Khởi động background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._listen_task = asyncio.create_task(self._listen_loop())

    # ── Heartbeat ────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        Gửi ping thủ công định kỳ ngoài ping_interval của websockets.
        Giúp giữ connection alive qua các proxy timeout ngắn (Cloudflare ~100s).
        """
        while not self._is_closing:
            await asyncio.sleep(settings.COMFY_WS_PING_INTERVAL)
            if self._ws and not self._ws.closed:
                try:
                    await self._ws.ping()
                    logger.debug("[comfy-ws] heartbeat ping sent")
                except Exception as e:
                    logger.debug(f"[comfy-ws] heartbeat ping failed: {e}")
                    break

    # ── Listen loop ───────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """
        Nhận message real-time từ ComfyUI WebSocket.

        ComfyUI gửi nhiều loại event:
          - status          : queue status
          - progress        : step progress
          - executing       : node đang chạy
          - execution_cached: node dùng cache
          - executed        : 1 node hoàn thành (có output files)
          - execution_success / execution_complete : toàn bộ prompt xong
          - execution_error : lỗi

        Ta chỉ cần lắng nghe execution_success / execution_error để resolve Future.
        """
        try:
            async for raw_message in self._ws:
                try:
                    import json
                    data: dict = json.loads(raw_message)
                    msg_type: str = data.get("type", "")
                    msg_data: dict = data.get("data", {})

                    # prompt_id có thể nằm trong data hoặc top-level
                    prompt_id: str = msg_data.get("prompt_id") or data.get("prompt_id", "")

                    logger.debug(
                        f"[comfy-ws] ← {msg_type} | prompt_id={prompt_id or '?'}"
                    )

                    # ── Xử lý event kết thúc ────────────────────────────
                    is_completed = False
                    is_error = False

                    if msg_type == "execution_error":
                        is_error = True
                    elif msg_type == "executing" and msg_data.get("node") is None:
                        is_completed = True
                    elif msg_type in ("execution_success", "execution_complete", "execution_completed"):
                        is_completed = True

                    if is_completed or is_error:
                        if prompt_id and prompt_id in self._listeners:
                            future = self._listeners.pop(prompt_id, None)
                            if future and not future.done():
                                if is_error:
                                    # Ghi lỗi vào Future để caller xử lý
                                    err_msg = msg_data.get("exception_message", "Unknown ComfyUI error")
                                    future.set_exception(
                                        RuntimeError(f"ComfyUI execution_error: {err_msg}")
                                    )
                                    logger.error(
                                        f"[comfy-ws] ❌ Prompt {prompt_id} error: {err_msg}"
                                    )
                                else:
                                    future.set_result(data)
                                    logger.info(
                                        f"[comfy-ws] ✅ Prompt {prompt_id} completed (real-time)"
                                    )

                except Exception as parse_err:
                    logger.warning(f"[comfy-ws] Message parse error: {parse_err}")

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"[comfy-ws] Connection closed: code={e.code} reason={e.reason}")
        except Exception as e:
            logger.error(f"[comfy-ws] Listen loop error: {e}")
        finally:
            self._connected = False
            # Trigger reconnect nếu chưa đóng chủ động
            if not self._is_closing:
                logger.info("[comfy-ws] Scheduling reconnect...")
                asyncio.create_task(self._reconnect())

    # ── Reconnect ─────────────────────────────────────────────────────────

    async def _reconnect(self) -> None:
        """Reconnect sau khi connection bị đứt. Dùng exponential backoff."""
        backoff = 2
        for attempt in range(1, settings.COMFY_WS_RECONNECT_MAX_ATTEMPTS + 1):
            await asyncio.sleep(backoff)
            try:
                await self.connect()
                logger.info(f"[comfy-ws] Reconnected (attempt {attempt})")
                return
            except Exception as e:
                logger.warning(f"[comfy-ws] Reconnect attempt {attempt} failed: {e}")
                backoff = min(backoff * 2, 30)  # exponential backoff, tối đa 30s

        logger.error("[comfy-ws] All reconnect attempts exhausted")
        # Fail tất cả futures đang chờ
        for prompt_id, future in list(self._listeners.items()):
            if not future.done():
                future.set_exception(
                    RuntimeError("WebSocket reconnect exhausted — connection lost")
                )
        self._listeners.clear()

    # ── Wait for completion ───────────────────────────────────────────────

    async def wait_for_completion(
        self, prompt_id: str, timeout_sec: Optional[int] = None
    ) -> dict:
        """
        Chờ real-time cho đến khi ComfyUI hoàn thành prompt.

        Tạo 1 asyncio.Future, đăng ký vào _listeners.
        _listen_loop sẽ resolve Future khi nhận execution_success/error.

        Returns:
            dict: raw WebSocket message data (type + data fields).
        Raises:
            TimeoutError: nếu vượt quá timeout_sec.
            RuntimeError: nếu ComfyUI báo execution_error.
        """
        if timeout_sec is None:
            timeout_sec = settings.COMFY_RESULT_TIMEOUT_SEC

        if not self._connected or not self._ws or self._ws.closed:
            await self.connect()

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._listeners[prompt_id] = future

        try:
            result = await asyncio.wait_for(future, timeout=timeout_sec)
            return result
        except asyncio.TimeoutError:
            logger.warning(
                f"[comfy-ws] Timeout {timeout_sec}s waiting for prompt_id={prompt_id}"
            )
            raise TimeoutError(
                f"WebSocket timeout after {timeout_sec}s for prompt_id={prompt_id}"
            )
        finally:
            # Cleanup: xóa listener dù thành công hay lỗi
            self._listeners.pop(prompt_id, None)

    # ── Close ─────────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Đóng WebSocket connection và huỷ background tasks."""
        self._is_closing = True
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
        if self._ws and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._connected = False
        logger.info(f"[comfy-ws] Closed (client_id={self.client_id})")


# ─────────────────────────── Ready check ───────────────────────────

async def wait_comfyui_ready(
    endpoint: str, pod_id: str | None = None, timeout_sec: int | None = None
) -> bool:
    """
    Poll ComfyUI /queue mỗi 5 giây cho đến khi trả 200 hoặc timeout.
    pod_id được giữ lại cho tương thích nhưng không dùng podExec
    (podExec trả 400 trên Community Cloud — không hỗ trợ).
    """
    if timeout_sec is None:
        timeout_sec = settings.COMFY_READY_TIMEOUT_SEC

    started = time.monotonic()
    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}

    while time.monotonic() - started < timeout_sec:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{endpoint}/queue", headers=headers)
                if r.status_code == 200:
                    logger.info(f"[comfy] ✅ ComfyUI ready at {endpoint}")
                    return True
                logger.debug(f"[comfy] /queue → {r.status_code}, retrying...")
        except Exception as e:
            logger.debug(f"[comfy] ping error: {e}")

        await asyncio.sleep(5)

    logger.warning(
        f"[comfy] ⚠️  ComfyUI at {endpoint} did not become ready in {timeout_sec}s"
    )
    return False


# ─────────────────────────── Submit workflow ───────────────────────────

async def submit_workflow(endpoint: str, workflow: dict, client_id: str) -> str:
    """
    Submit workflow JSON tới ComfyUI /prompt.
    Trả về prompt_id (UUID do ComfyUI sinh).
    client_id nên là job_id của Dispatcher để dễ debug và map với WS listener.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.RUNPOD_API_KEY}",
    }
    payload = {"prompt": workflow, "client_id": client_id}

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{endpoint}/prompt", json=payload, headers=headers)

    if r.status_code != 200:
        raise RuntimeError(f"ComfyUI /prompt returned {r.status_code}: {r.text[:300]}")

    data = r.json()

    # ComfyUI có thể trả lỗi validation trong body (status 200 nhưng có error)
    if data.get("error"):
        raise RuntimeError(f"ComfyUI workflow error: {data['error']}")

    prompt_id = data.get("prompt_id")
    if not prompt_id:
        raise RuntimeError(f"Missing prompt_id in ComfyUI response: {data}")

    logger.info(f"[comfy] submitted → prompt_id={prompt_id}")
    return prompt_id


# ─────────────────────────── Wait for result (WS + fallback) ───────────────────────────

async def wait_for_result(
    endpoint: str,
    prompt_id: str,
    client_id: str,
    timeout_sec: int | None = None,
) -> dict:
    """
    Chờ ComfyUI hoàn thành render bằng WebSocket (real-time).
    Tự động fallback về polling HTTP nếu WebSocket không khả dụng.

    Args:
        endpoint:    ComfyUI base URL (http/https).
        prompt_id:   ID trả về từ submit_workflow.
        client_id:   Dùng làm WS client_id (nên là job_id).
        timeout_sec: Timeout tổng thể (mặc định COMFY_RESULT_TIMEOUT_SEC).

    Returns:
        dict: history_item (output files, status, etc.) từ /history/{prompt_id}.
    """
    if timeout_sec is None:
        timeout_sec = settings.COMFY_RESULT_TIMEOUT_SEC

    t_start = time.monotonic()
    ws_client = AsyncComfyWebSocketClient(endpoint, client_id)

    try:
        # ── Thử WebSocket trước ─────────────────────────────────────────
        logger.info(
            f"[comfy] Connecting WebSocket for prompt_id={prompt_id} "
            f"(timeout={timeout_sec}s)"
        )
        await ws_client.connect()

        # Chờ event real-time (execution_success / execution_error)
        await ws_client.wait_for_completion(prompt_id, timeout_sec=timeout_sec)

        # WS chỉ báo "xong" — cần fetch history để lấy output files
        elapsed = time.monotonic() - t_start
        logger.info(
            f"[comfy] WS completed in {elapsed:.1f}s → fetching history "
            f"for prompt_id={prompt_id}"
        )
        return await _fetch_history(endpoint, prompt_id)

    except Exception as ws_err:
        # ── Fallback về polling ─────────────────────────────────────────
        elapsed_so_far = time.monotonic() - t_start
        remaining = max(10, timeout_sec - int(elapsed_so_far))
        logger.warning(
            f"[comfy] WebSocket failed after {elapsed_so_far:.1f}s: {ws_err} "
            f"→ fallback to polling (remaining={remaining}s)"
        )
        return await poll_result(endpoint, prompt_id, timeout_sec=remaining)

    finally:
        # Đảm bảo đóng WS dù thành công hay fail
        await ws_client.close()


# ─────────────────────────── Fetch history (sau khi WS báo xong) ───────────────────────────

async def _fetch_history(endpoint: str, prompt_id: str) -> dict:
    """
    Lấy full history item từ /history/{prompt_id} sau khi WS báo completed.
    Retry 3 lần vì đôi khi ComfyUI cần vài ms để ghi history sau khi fire event.
    """
    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}

    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(f"{endpoint}/history/{prompt_id}", headers=headers)
                r.raise_for_status()
                history = r.json()
                if history and prompt_id in history:
                    return history[prompt_id]
                # History chưa có → đợi chút
                logger.debug(
                    f"[comfy] history not yet available (attempt {attempt}/3), waiting 1s..."
                )
                await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"[comfy] _fetch_history attempt {attempt} error: {e}")
            await asyncio.sleep(1)

    raise RuntimeError(
        f"Could not fetch history for prompt_id={prompt_id} after 3 attempts"
    )


# ─────────────────────────── Poll history (fallback) ───────────────────────────

async def poll_result(
    endpoint: str, prompt_id: str, timeout_sec: int | None = None
) -> dict:
    """
    Fallback: Poll /history/{prompt_id} cho đến khi ComfyUI hoàn thành render.
    ComfyUI chỉ ghi vào history khi prompt đã chạy xong (thành công hoặc lỗi).

    Dùng khi WebSocket không khả dụng (WS handshake fail, proxy block, v.v.).
    """
    if timeout_sec is None:
        timeout_sec = settings.COMFY_RESULT_TIMEOUT_SEC

    interval = settings.COMFY_POLL_INTERVAL_SEC
    started = time.monotonic()
    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}
    elapsed_log = 0
    consecutive_404 = 0
    # 60 × 5s = 300s (5 phút) liên tục 404 → mới coi là crash
    # RunPod proxy có thể 404 vài phút khi model đang load — không nên fail sớm
    MAX_CONSECUTIVE_404 = 60

    while time.monotonic() - started < timeout_sec:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    f"{endpoint}/history/{prompt_id}", headers=headers
                )

            if r.status_code == 200:
                consecutive_404 = 0  # reset khi có response OK
                history = r.json()
                if history and prompt_id in history:
                    logger.info(
                        f"[comfy] ✅ (poll) result received for prompt_id={prompt_id}"
                    )
                    return history[prompt_id]
            else:
                consecutive_404 += 1
                logger.warning(
                    f"[comfy] poll /history → {r.status_code} "
                    f"(pod may be unreachable) [{consecutive_404}/{MAX_CONSECUTIVE_404}]: "
                    f"{r.text[:120]}"
                )
                # Fail fast: ComfyUI crash (404 liên tiếp > ngưỡng)
                if consecutive_404 >= MAX_CONSECUTIVE_404:
                    raise RuntimeError(
                        f"ComfyUI unreachable for {consecutive_404 * interval}s "
                        f"(got {r.status_code} × {consecutive_404}) — pod likely crashed"
                    )

        except RuntimeError:
            raise  # re-raise fail-fast error (không retry)
        except Exception as e:
            consecutive_404 += 1
            logger.warning(
                f"[comfy] poll error [{consecutive_404}/{MAX_CONSECUTIVE_404}]: {e}"
            )
            if consecutive_404 >= MAX_CONSECUTIVE_404:
                raise RuntimeError(
                    f"ComfyUI unreachable for {consecutive_404 * interval}s "
                    f"— pod likely crashed: {e}"
                )

        elapsed = int(time.monotonic() - started)
        if elapsed - elapsed_log >= 60:
            logger.info(
                f"[comfy] still polling prompt_id={prompt_id} ({elapsed}s elapsed)"
            )
            elapsed_log = elapsed

        await asyncio.sleep(interval)

    raise TimeoutError(
        f"ComfyUI result timeout after {timeout_sec}s for prompt_id={prompt_id}"
    )


# ─────────────────────────── Extract output files ───────────────────────────

def extract_output_files(history_item: dict) -> list[dict]:
    """
    Parse history item trả về danh sách output files.
    Hỗ trợ: videos, images, gifs.
    """
    outputs = history_item.get("outputs", {})
    files: list[dict] = []

    for node_id, node_output in outputs.items():
        for ftype, label in [("videos", "video"), ("images", "image"), ("gifs", "gif")]:
            for f in node_output.get(ftype, []):
                files.append({
                    "type":        label,
                    "filename":    f["filename"],
                    "subfolder":   f.get("subfolder", ""),
                    "folder_type": f.get("type", "output"),
                    "node_id":     node_id,
                })

    return files


def pick_primary_output(files: list[dict]) -> dict | None:
    """Ưu tiên video → image → gif."""
    for preferred in ("video", "image", "gif"):
        for f in files:
            if f["type"] == preferred:
                return f
    # Fallback: bất kỳ file nào có đuôi mp4/webm/png
    for f in files:
        if f["filename"].endswith((".mp4", ".webm", ".png", ".jpg")):
            return f
    return files[0] if files else None


def build_view_url(endpoint: str, file: dict) -> str:
    """Tạo URL download file từ ComfyUI /view endpoint."""
    params = urlencode({
        "filename":  file["filename"],
        "subfolder": file.get("subfolder", ""),
        "type":      file.get("folder_type", "output"),
    })
    return f"{endpoint}/view?{params}"


# ─────────────────────────── Public API ───────────────────────────

__all__ = [
    "AsyncComfyWebSocketClient",
    "wait_comfyui_ready",
    "submit_workflow",
    "wait_for_result",
    "poll_result",
    "extract_output_files",
    "pick_primary_output",
    "build_view_url",
]
