import asyncio
import time
import uuid
from loguru import logger
from redis_client import get_redis
from runpod_client import runpod
from worker_pool import pool
from config import settings


async def autoscale_loop():
    """Chạy mỗi AUTOSCALE_INTERVAL_SEC giây."""
    logger.info("[autoscale] started")
    while True:
        try:
            await _tick()
        except asyncio.CancelledError:
            logger.info("[autoscale] stopped")
            return
        except Exception as e:
            logger.exception(f"[autoscale] tick failed: {e}")
        await asyncio.sleep(settings.AUTOSCALE_INTERVAL_SEC)


async def _tick():
    r = await get_redis()
    queue_depth = await r.llen(settings.QUEUE_KEY)
    counts = await pool.count_by_status()

    logger.info(
        f"[autoscale] queue={queue_depth} "
        f"total={counts['total']} idle={counts['idle']} "
        f"busy={counts['busy']} booting={counts['booting']}"
    )

    # ---- Warm pool: giữ MIN_WORKERS (chỉ khi có job hoặc MIN_WORKERS > 0) ----
    if settings.MIN_WORKERS > 0 and queue_depth > 0:
        deficit = settings.MIN_WORKERS - counts["total"]
        for _ in range(max(0, deficit)):
            if counts["total"] < settings.MAX_WORKERS:
                await scale_up()
                counts["total"] += 1

    # ---- Scale up theo queue ----
    available = counts["idle"] + counts["booting"]
    if queue_depth > 0 and available == 0:
        if counts["total"] < settings.MAX_WORKERS:
            await scale_up()

    # ---- Scale down idle workers ----
    await _scale_down(counts)


async def scale_up():
    name = f"comfy-worker-{uuid.uuid4().hex[:8]}"
    logger.info(f"[autoscale] SCALE UP → creating {name}")
    try:
        pod = await runpod.create_pod(name)
        pod_id = pod["id"]
        logger.info(f"[autoscale] created pod {pod_id} (status={pod.get('desiredStatus')})")
        # Ghi ngay vào registry với status=booting để autoscaler không tạo thêm pod trùng
        await pool.mark_booting(pod_id)
        return pod
    except Exception as e:
        logger.error(f"[autoscale] scale up FAILED: {e}")
        return None


async def _scale_down(counts: dict):
    if counts["total"] <= settings.MIN_WORKERS:
        return
    now = int(time.time())
    workers = await pool.list_workers()
    idle_workers = sorted(
        [
            w for w in workers
            if w["status"] == "idle"
            and now - w["last_active"] > settings.IDLE_TIMEOUT_SEC
        ],
        key=lambda w: w["last_active"],  # kill cái idle lâu nhất trước
    )
    excess = counts["total"] - settings.MIN_WORKERS
    to_kill = idle_workers[:excess]
    for w in to_kill:
        logger.info(f"[autoscale] SCALE DOWN → terminating {w['pod_id']}")
        try:
            await runpod.terminate_pod(w["pod_id"])
            await pool.remove(w["pod_id"])
        except Exception as e:
            logger.error(f"[autoscale] terminate failed: {e}")
