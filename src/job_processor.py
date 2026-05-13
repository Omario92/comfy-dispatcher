"""
job_processor.py — Background pipeline xử lý job async.

Flow:
  queued → starting_pod → waiting_comfyui → running → done / failed

Dispatcher gọi: asyncio.create_task(process_job(job_id))  ← không await
Không bao giờ block HTTP request của n8n.
"""
import asyncio
import json

import httpx
from loguru import logger

from autoscaler import scale_up
from comfy_client import (
    build_view_url,
    extract_output_files,
    pick_primary_output,
    poll_result,
    submit_workflow,
    wait_comfyui_ready,
)
from config import settings
from job_store import jobs
from r2_uploader import download_and_upload_r2
from worker_pool import pool


# ─────────────────────────── Entry point ───────────────────────────

async def process_job(job_id: str) -> None:
    """
    Pipeline đầy đủ chạy trong background task.
    Mọi exception đều được catch và ghi vào job status.
    """
    logger.info(f"[processor] ▶ start job={job_id}")

    pod_id = ""
    prompt_id = ""

    try:
        # ── Step 1: Lấy worker idle (hoặc scale up) ──────────────────
        await jobs.set_status(job_id, "starting_pod")
        worker = await _acquire_worker(job_id)
        pod_id = worker["pod_id"]

        # MARK BUSY NGAY LẬP TỨC để tránh bị Autoscaler kill trong khi đợi boot
        await pool.mark_busy(pod_id, job_id)

        comfy_endpoint = (
            worker.get("proxy_url")
            or f"https://{pod_id}-8188.proxy.runpod.net"
        )

        await jobs.set_waiting_comfy(job_id, pod_id, comfy_endpoint)
        logger.info(f"[processor] job={job_id} → pod={pod_id} reserved (busy)")

        # ── Step 2: Đợi ComfyUI ready ────────────────────────────────
        ready = await wait_comfyui_ready(
            comfy_endpoint, timeout_sec=settings.COMFY_READY_TIMEOUT_SEC
        )
        if not ready:
            raise TimeoutError(
                f"ComfyUI at {comfy_endpoint} did not become ready "
                f"within {settings.COMFY_READY_TIMEOUT_SEC}s"
            )

        # ── Step 3: Update job status (pod đã busy từ bước 1) ──────────
        await jobs.set_status(job_id, "running")

        # ── Step 4: Load workflow từ Redis ───────────────────────────
        job_data = await jobs.get(job_id)
        if not job_data:
            raise RuntimeError(f"Job {job_id} disappeared from store")

        raw_workflow = job_data.get("workflow", "")
        if not raw_workflow:
            raise ValueError(f"Job {job_id} has no workflow data")

        workflow: dict = json.loads(raw_workflow) if isinstance(raw_workflow, str) else raw_workflow

        # ── Step 5: Submit workflow → lấy prompt_id ──────────────────
        prompt_id = await submit_workflow(comfy_endpoint, workflow, client_id=job_id)
        await jobs.update_prompt_id(job_id, prompt_id)
        logger.info(f"[processor] job={job_id} prompt_id={prompt_id}")

        # ── Step 6: Poll /history/{prompt_id} ────────────────────────
        history_item = await poll_result(
            comfy_endpoint, prompt_id,
            timeout_sec=settings.COMFY_RESULT_TIMEOUT_SEC,
        )

        # ── Step 7: Extract output file ──────────────────────────────
        files = extract_output_files(history_item)
        output_file = pick_primary_output(files)

        if not output_file:
            raise ValueError(
                f"No output file found in ComfyUI history for prompt_id={prompt_id}"
            )

        logger.info(
            f"[processor] output file: {output_file['filename']} "
            f"type={output_file['type']}"
        )

        # ── Step 8: Download + upload R2 ─────────────────────────────
        view_url = build_view_url(comfy_endpoint, output_file)
        ext = output_file["filename"].rsplit(".", 1)[-1] if "." in output_file["filename"] else "mp4"
        content_type = _guess_content_type(ext)
        r2_key = f"outputs/{job_id}/{output_file['filename']}"

        result_url = await download_and_upload_r2(view_url, r2_key, content_type)

        # ── Step 9: Mark done ─────────────────────────────────────────
        personality = job_data.get("personality", "")
        await jobs.set_done(job_id, result_url, img_personality=str(personality))
        logger.info(f"[processor] ✅ job={job_id} done → {result_url}")

        # ── Step 10: Callback n8n ─────────────────────────────────────
        await _callback_n8n(job_id, "done", result_url, None, pod_id, prompt_id)

    except Exception as e:
        err_msg = f"{type(e).__name__}: {str(e)}"
        logger.exception(f"[processor] ❌ job={job_id} FAILED: {err_msg}")
        await jobs.set_failed(job_id, err_msg)
        await _callback_n8n(job_id, "failed", None, err_msg, pod_id, prompt_id)

    finally:
        # Luôn trả pod về idle khi xong, kể cả khi lỗi
        if pod_id:
            try:
                await pool.mark_idle(pod_id)
                logger.info(f"[processor] pod={pod_id} marked idle")
            except Exception as e:
                logger.error(f"[processor] failed to mark pod idle: {e}")


# ─────────────────────────── Helpers ───────────────────────────

async def _acquire_worker(job_id: str) -> dict:
    """
    Đợi idle worker. Nếu chưa có, trigger scale_up.
    Retry scale_up mỗi 60 giây nếu vẫn không có worker.
    Timeout = BOOT_TIMEOUT_SEC.
    """
    SCALE_UP_RETRY_INTERVAL = 60  # retry scale_up mỗi N giây

    async def _try_scale_up(reason: str = ""):
        counts = await pool.count_by_status()
        if counts["total"] < settings.MAX_WORKERS:
            logger.info(f"[processor] scale_up triggered for {job_id} ({reason})")
            try:
                await scale_up()
            except Exception as e:
                logger.warning(f"[processor] scale_up failed: {e}")

    # Lần đầu: trigger ngay
    await _try_scale_up("no workers on arrival")

    timeout = settings.BOOT_TIMEOUT_SEC
    last_scale_up = 0  # seconds elapsed tại lần scale_up cuối

    for elapsed in range(timeout):
        worker = await pool.get_idle_worker()
        if worker:
            return worker

        # Retry scale_up mỗi SCALE_UP_RETRY_INTERVAL giây
        if elapsed > 0 and elapsed - last_scale_up >= SCALE_UP_RETRY_INTERVAL:
            counts = await pool.count_by_status()
            # Chỉ retry nếu thực sự không có pod nào đang boot/running
            if counts["idle"] + counts["booting"] == 0:
                await _try_scale_up(f"retry at {elapsed}s, still no workers")
                last_scale_up = elapsed

        if elapsed % 30 == 0:
            counts = await pool.count_by_status()
            logger.info(
                f"[processor] waiting for idle worker "
                f"({elapsed}/{timeout}s) for job={job_id} | "
                f"total={counts['total']} idle={counts['idle']} "
                f"booting={counts['booting']} stopped={counts['stopped']}"
            )
        await asyncio.sleep(1)

    raise TimeoutError(
        f"No idle worker available after {timeout}s for job={job_id}"
    )


async def _callback_n8n(
    job_id: str,
    status: str,
    result_url: str | None,
    error: str | None,
    pod_id: str,
    prompt_id: str,
) -> None:
    """POST callback tới n8n webhook (per-job callback_url có ưu tiên cao hơn global N8N_CALLBACK_URL)."""
    job_data = await jobs.get(job_id)

    # Ưu tiên callback_url được set trong job, fallback sang global setting
    callback_url = (job_data or {}).get("callback_url", "") or settings.N8N_CALLBACK_URL
    if not callback_url:
        logger.debug(f"[processor] no callback_url for job={job_id}, skip")
        return

    payload = {
        "job_id":          job_id,
        "status":          status,
        "result_url":      result_url,
        "error":           error,
        "pod_id":          pod_id,
        "comfy_prompt_id": prompt_id,
        "personality":     (job_data or {}).get("personality", ""),
        "user_id":         (job_data or {}).get("user_id", ""),
        "updated_at":      (job_data or {}).get("updated_at", ""),
    }

    logger.info(f"[processor] callback → {callback_url} status={status}")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(callback_url, json=payload)
            if r.status_code >= 400:
                logger.warning(
                    f"[processor] callback returned {r.status_code}: {r.text[:200]}"
                )
    except Exception as e:
        logger.error(f"[processor] callback failed: {e}")


def _guess_content_type(ext: str) -> str:
    mapping = {
        "mp4":  "video/mp4",
        "webm": "video/webm",
        "gif":  "image/gif",
        "png":  "image/png",
        "jpg":  "image/jpeg",
        "jpeg": "image/jpeg",
    }
    return mapping.get(ext.lower(), "application/octet-stream")
