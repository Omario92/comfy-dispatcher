"""
comfy_client.py — Tương tác trực tiếp với ComfyUI qua RunPod Proxy URL.

RunPod proxy format: https://{pod_id}-8188.proxy.runpod.net
Cloudflare giới hạn 100s per request → phải poll, không block.
"""
import asyncio
from urllib.parse import urlencode

import httpx
from loguru import logger

from config import settings


# ─────────────────────────── Ready check ───────────────────────────

async def wait_comfyui_ready(endpoint: str, pod_id: str | None = None, timeout_sec: int | None = None) -> bool:
    """
    Poll ComfyUI /queue mỗi 5 giây cho đến khi trả 200 hoặc timeout.
    pod_id được giữ lại cho tương thích nhưng không dùng podExec
    (podExec trả 400 trên Community Cloud — không hỗ trợ).
    """
    if timeout_sec is None:
        timeout_sec = settings.COMFY_READY_TIMEOUT_SEC

    started = asyncio.get_event_loop().time()
    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}

    while asyncio.get_event_loop().time() - started < timeout_sec:
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

    logger.warning(f"[comfy] ⚠️  ComfyUI at {endpoint} did not become ready in {timeout_sec}s")
    return False


# ─────────────────────────── Submit workflow ───────────────────────────

async def submit_workflow(endpoint: str, workflow: dict, client_id: str) -> str:
    """
    Submit workflow JSON tới ComfyUI /prompt.
    Trả về prompt_id (UUID do ComfyUI sinh).
    client_id nên là job_id của Dispatcher để dễ debug.
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

    # ComfyUI có thể trả lỗi validation trong body
    if data.get("error"):
        raise RuntimeError(f"ComfyUI workflow error: {data['error']}")

    prompt_id = data.get("prompt_id")
    if not prompt_id:
        raise RuntimeError(f"Missing prompt_id in ComfyUI response: {data}")

    logger.info(f"[comfy] submitted → prompt_id={prompt_id}")
    return prompt_id


# ─────────────────────────── Poll history ───────────────────────────

async def poll_result(endpoint: str, prompt_id: str, timeout_sec: int | None = None) -> dict:
    """
    Poll /history/{prompt_id} cho đến khi ComfyUI hoàn thành render.
    ComfyUI chỉ ghi vào history khi prompt đã chạy xong (thành công hoặc lỗi).
    """
    if timeout_sec is None:
        timeout_sec = settings.COMFY_RESULT_TIMEOUT_SEC

    interval  = settings.COMFY_POLL_INTERVAL_SEC
    started   = asyncio.get_event_loop().time()
    headers   = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}
    elapsed_log     = 0
    consecutive_404 = 0
    # 60 × 5s = 300s (5 phút) liên tục 404 → mới coi là crash
    # RunPod proxy có thể 404 vài phút khi model đang load — không nên fail sớm
    MAX_CONSECUTIVE_404 = 60

    while asyncio.get_event_loop().time() - started < timeout_sec:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(f"{endpoint}/history/{prompt_id}", headers=headers)

            if r.status_code == 200:
                consecutive_404 = 0  # reset khi có response OK
                history = r.json()
                if history and prompt_id in history:
                    logger.info(f"[comfy] ✅ result received for prompt_id={prompt_id}")
                    return history[prompt_id]
            else:
                consecutive_404 += 1
                logger.warning(
                    f"[comfy] poll /history returned {r.status_code} "
                    f"(pod may be unreachable) [{consecutive_404}/{MAX_CONSECUTIVE_404}]: {r.text[:120]}"
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
            logger.warning(f"[comfy] poll error [{consecutive_404}/{MAX_CONSECUTIVE_404}]: {e}")
            if consecutive_404 >= MAX_CONSECUTIVE_404:
                raise RuntimeError(
                    f"ComfyUI unreachable for {consecutive_404 * interval}s — pod likely crashed: {e}"
                )

        elapsed = int(asyncio.get_event_loop().time() - started)
        if elapsed - elapsed_log >= 60:
            logger.info(f"[comfy] still polling prompt_id={prompt_id} ({elapsed}s elapsed)")
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
        "filename": file["filename"],
        "subfolder": file.get("subfolder", ""),
        "type": file.get("folder_type", "output"),
    })
    return f"{endpoint}/view?{params}"
