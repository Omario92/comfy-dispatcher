import asyncio
import json
import httpx
from loguru import logger
from redis_client import get_redis
from worker_pool import pool
from job_store import jobs
from config import settings


async def consumer_loop():
    """Pull job từ Redis queue, assign tới idle worker."""
    r = await get_redis()
    logger.info("[consumer] started")
    while True:
        try:
            # block 5s đợi job
            res = await r.brpop(settings.QUEUE_KEY, timeout=5)
            if res is None:
                continue
            _, raw = res
            try:
                job = json.loads(raw)
            except json.JSONDecodeError:
                logger.error(f"[consumer] DISCARD invalid JSON message: {raw!r}")
                continue  # bỏ qua message lỗi, không requeue
            await _dispatch(job)
        except asyncio.CancelledError:
            logger.info("[consumer] stopped")
            return
        except Exception as e:
            logger.exception(f"[consumer] error: {e}")
            await asyncio.sleep(1)


async def _dispatch(job: dict):
    job_id = job["job_id"]
    logger.info(f"[consumer] dispatching job {job_id}")

    # Nếu không có worker nào sẵn sàng (idle/booting) và chưa max, tự trigger scale up
    counts = await pool.count_by_status()
    if (counts["idle"] + counts["booting"] == 0) and (counts["total"] < settings.MAX_WORKERS):
        logger.info(f"[consumer] trigger proactive scale_up for job {job_id}")
        from autoscaler import scale_up
        asyncio.create_task(scale_up())

    # Đợi idle worker (max 120s, đủ cho pod boot ~60-90s)
    worker = None
    for i in range(120):
        worker = await pool.get_idle_worker()
        if worker:
            break
        if i % 10 == 0:
            logger.info(f"[consumer] waiting for idle worker... ({i}s)")
        await asyncio.sleep(1)

    if not worker:
        logger.warning(f"[consumer] no idle worker for {job_id}, requeue")
        await _requeue(job)
        return

    # Mark busy + persist job state
    await pool.mark_busy(worker["pod_id"], job_id)
    await jobs.set_processing(job_id, worker["pod_id"])

    try:
        await _send_to_worker(worker, job)
    except Exception as e:
        logger.error(f"[consumer] send failed: {e}")
        await pool.mark_idle(worker["pod_id"])
        # Retry up to 2 times
        job["retries"] = job.get("retries", 0) + 1
        if job["retries"] < 3:
            await _requeue(job)
        else:
            await jobs.set_failed(job_id, f"max retries exceeded: {e}")


async def _send_to_worker(worker: dict, job: dict):
    """Gửi job tới worker: ưu tiên proxy_url (ComfyUI API), fallback direct ip:port."""
    proxy_url = worker.get("proxy_url")

    if proxy_url:
        # Gửi workflow trực tiếp vào ComfyUI /prompt API qua RunPod proxy
        url = f"{proxy_url}/prompt"
        payload = {
            "prompt": job.get("workflow", {}),
            "client_id": job["job_id"],
        }
        logger.info(f"[consumer] sending job {job['job_id']} to ComfyUI proxy {proxy_url}")
    else:
        # Worker Agent mode (port 9000)
        url = f"http://{worker['ip']}:{worker['port']}/run_job"
        payload = {
            "job_id": job["job_id"],
            "workflow": job["workflow"],
            "personality": job.get("personality", 0),
            "callback_url": f"{settings.DISPATCHER_PUBLIC_URL}/worker/done",
        }
        logger.info(f"[consumer] sending job {job['job_id']} to worker agent {worker['pod_id']}")

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        logger.info(f"[consumer] job {job['job_id']} accepted by {worker['pod_id']}")


async def _requeue(job: dict):
    r = await get_redis()
    await r.lpush(settings.QUEUE_KEY, json.dumps(job))
