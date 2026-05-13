import asyncio
import time
import uuid
from datetime import datetime, timezone, timedelta
from loguru import logger
from redis_client import get_redis
from runpod_client import runpod
from worker_pool import pool
from config import settings

# Vietnam = UTC+7
_VN_TZ = timezone(timedelta(hours=7))


def peak_hours_status() -> tuple[bool, int, int]:
    """
    Kiểm tra giờ hiện tại (giờ VN) có phải peak hours không.

    Returns:
        (is_peak, idle_timeout_sec, min_workers)
    """
    # Nếu chưa cấu hình peak hours → chế độ testing, dùng IDLE_TIMEOUT_SEC thẺ
    if settings.PEAK_HOURS_START is None or settings.PEAK_HOURS_END is None:
        return False, settings.IDLE_TIMEOUT_SEC, settings.MIN_WORKERS

    now_vn = datetime.now(_VN_TZ)
    hour = now_vn.hour  # 0-23

    is_peak = settings.PEAK_HOURS_START <= hour < settings.PEAK_HOURS_END

    if is_peak:
        return True, settings.PEAK_IDLE_TIMEOUT_SEC, settings.PEAK_MIN_WORKERS
    else:
        return False, settings.OFF_PEAK_IDLE_TIMEOUT_SEC, settings.MIN_WORKERS


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

    is_peak, idle_timeout, min_workers = peak_hours_status()

    logger.info(
        f"[autoscale] queue={queue_depth} "
        f"total={counts['total']} idle={counts['idle']} "
        f"busy={counts['busy']} booting={counts['booting']} | "
        f"{'PEAK' if is_peak else 'off-peak'} "
        f"idle_timeout={idle_timeout}s min_workers={min_workers}"
    )

    # ---- Warm pool: giữ min_workers (trong peak giữ PEAK_MIN_WORKERS) ----
    if min_workers > 0:
        deficit = min_workers - counts["total"]
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
    await _scale_down(counts, idle_timeout, min_workers)


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


async def _scale_down(counts: dict, idle_timeout: int, min_workers: int):
    if counts["total"] <= min_workers:
        return
    now = int(time.time())
    workers = await pool.list_workers()
    idle_workers = sorted(
        [
            w for w in workers
            if w["status"] == "idle"
            and now - w["last_active"] > idle_timeout
        ],
        key=lambda w: w["last_active"],  # kill cái idle lâu nhất trước
    )
    excess = counts["total"] - min_workers
    to_kill = idle_workers[:excess]
    for w in to_kill:
        logger.info(
            f"[autoscale] SCALE DOWN → terminating {w['pod_id']} "
            f"(idle {now - w['last_active']}s > timeout {idle_timeout}s)"
        )
        try:
            await runpod.terminate_pod(w["pod_id"])
            await pool.remove(w["pod_id"])
        except Exception as e:
            logger.error(f"[autoscale] terminate failed: {e}")

