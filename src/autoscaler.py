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

    # Đọc pending counter theo type (tăng/giảm bởi job_processor)
    image_pending = int(await r.get(settings.IMAGE_PENDING_KEY) or 0)
    video_pending = int(await r.get(settings.VIDEO_PENDING_KEY) or 0)

    is_peak, idle_timeout, min_workers = peak_hours_status()

    # Đếm pod active (idle + booting) theo type
    # QUAN TRỌNG: phải kể cả pod ĐANG BOOT, không chỉ idle
    # → tránh vòng lặp tạo 10 pod cho 1 job khi pod vẫn đang boot
    active_by_type = await pool.count_active_by_type()
    active_image = active_by_type.get("image", 0) + active_by_type.get("any", 0)
    active_video = active_by_type.get("video", 0) + active_by_type.get("any", 0)

    logger.info(
        f"[autoscale] queue={queue_depth} (img_pending={image_pending} vid_pending={video_pending}) "
        f"total={counts['total']} idle={counts['idle']} "
        f"busy={counts['busy']} booting={counts['booting']} "
        f"stopped={counts['stopped']} | "
        f"active_image={active_image} active_video={active_video} | "
        f"{'PEAK' if is_peak else 'off-peak'} "
        f"pause={settings.PAUSE_TIMEOUT_SEC}s "
        f"terminate={settings.TERMINATE_TIMEOUT_SEC}s "
        f"min_workers={min_workers}"
    )

    # ---- Warm pool: giữ min_workers trong peak ----
    if min_workers > 0:
        active = counts["idle"] + counts["booting"] + counts["busy"]
        deficit = min_workers - active
        for _ in range(max(0, deficit)):
            if counts["total"] < settings.MAX_WORKERS:
                await scale_up()
                counts["total"] += 1

    # NOTE: Type-specific scale_up (image/video) được xử lý bởi job_processor._acquire_worker
    # Autoscaler KHÔNG gọi scale_up theo type để tránh tạo pod trùng với job_processor.
    # Autoscaler chỉ log trạng thái pending để monitoring.
    if image_pending > 0 and active_image == 0:
        logger.warning(f"[autoscale] WARNING: {image_pending} image jobs pending but no active image pod — job_processor should handle scale_up")
    if video_pending > 0 and active_video == 0:
        logger.warning(f"[autoscale] WARNING: {video_pending} video jobs pending but no active video pod — job_processor should handle scale_up")


    # Fallback: nếu có job trong queue mà không có pod nào sẵn → resume stopped hoặc scale up "any"
    available_total = counts["idle"] + counts["booting"]
    if queue_depth > 0 and available_total == 0:
        stopped = await pool.get_stopped_workers()
        if stopped:
            # Resume pod stopped đầu tiên (nhanh hơn tạo mới)
            pod_to_resume = stopped[0]
            logger.info(f"[autoscale] RESUME → {pod_to_resume['pod_id']} (stopped pod, faster than new)")
            try:
                ok = await runpod.resume_pod(pod_to_resume["pod_id"])
                if ok:
                    await pool.mark_booting(pod_to_resume["pod_id"],
                                            worker_type=pod_to_resume.get("worker_type", "any"))

            except Exception as e:
                logger.error(f"[autoscale] resume failed: {e}")
        elif counts["total"] < settings.MAX_WORKERS:
            await scale_up()

    # ---- 2-phase scale down idle workers ----
    await _scale_down_two_phase(counts, min_workers)


async def scale_up(worker_type: str = "any") -> dict | None:
    """
    Tạo pod mới trên RunPod.

    worker_type:
      - "image" → pod đặt tên prefix "image-worker-"
      - "video" → pod đặt tên prefix "video-worker-"
      - "any"   → pod đặt tên prefix "comfy-worker-" (backward-compat)
    """
    prefix_map = {
        "image": "image-worker",
        "video": "video-worker",
        "any":   "comfy-worker",
    }
    prefix = prefix_map.get(worker_type, "comfy-worker")
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"

    logger.info(f"[autoscale] SCALE UP → creating {name} (worker_type={worker_type})")
    try:
        pod = await runpod.create_pod(name)
        pod_id = pod["id"]
        logger.info(f"[autoscale] created pod {pod_id} name={name} (status={pod.get('desiredStatus')})")
        # Ghi ngay vào registry với status=booting + worker_type
        await pool.mark_booting(pod_id, worker_type=worker_type)
        return pod
    except Exception as e:
        logger.error(f"[autoscale] scale up FAILED ({worker_type}): {e}")
        return None


async def _scale_down_two_phase(counts: dict, min_workers: int):
    """
    2-phase idle lifecycle:
      Phase 1: idle > PAUSE_TIMEOUT_SEC  → podStop (giải phóng GPU, giữ /workspace)
      Phase 2: idle > TERMINATE_TIMEOUT_SEC → podTerminate (xóa hẳn)
    """
    # Tính tổng pod "active" (không kể stopped) để so min_workers
    active_count = counts["idle"] + counts["busy"] + counts["booting"]
    if active_count <= min_workers and counts["stopped"] == 0:
        return

    now = int(time.time())
    workers = await pool.list_workers()

    for w in workers:
        status = w.get("status")
        pod_id = w["pod_id"]
        idle_sec = now - w.get("last_active", 0)

        if status == "idle":
            # Bỏ qua pod được ghim (pinned) cho VIP warmup
            if w.get("pinned_until", 0) > now:
                remaining = w["pinned_until"] - now
                logger.debug(f"[autoscale] pod {pod_id} is PINNED, {remaining}s remaining — skipping")
                continue

            if idle_sec > settings.TERMINATE_TIMEOUT_SEC and active_count > min_workers:
                # Phase 2: idle quá lâu → terminate hẳn
                logger.info(
                    f"[autoscale] TERMINATE → {pod_id} "
                    f"(idle {idle_sec}s > terminate_timeout {settings.TERMINATE_TIMEOUT_SEC}s)"
                )
                try:
                    await runpod.terminate_pod(pod_id)
                    await pool.remove(pod_id)
                    active_count -= 1
                except LookupError:
                    # Pod không tồn tại trên RunPod → xóa khỏi registry ngay
                    logger.warning(f"[autoscale] ghost pod {pod_id} (POD_NOT_FOUND) → removing from registry")
                    await pool.remove(pod_id)
                    active_count -= 1
                except Exception as e:
                    logger.error(f"[autoscale] terminate failed: {e}")

            elif idle_sec > settings.PAUSE_TIMEOUT_SEC:
                # Phase 1: idle vừa đủ → stop pod (giữ /workspace)
                logger.info(
                    f"[autoscale] STOP (pause) → {pod_id} "
                    f"(idle {idle_sec}s > pause_timeout {settings.PAUSE_TIMEOUT_SEC}s)"
                )
                try:
                    ok = await runpod.stop_pod(pod_id)
                    if ok:
                        await pool.mark_stopped(pod_id)
                        active_count -= 1
                except LookupError:
                    # Pod không tồn tại trên RunPod → xóa khỏi registry ngay
                    logger.warning(f"[autoscale] ghost pod {pod_id} (POD_NOT_FOUND) → removing from registry")
                    await pool.remove(pod_id)
                    active_count -= 1
                except Exception as e:
                    logger.error(f"[autoscale] stop failed: {e}")

        elif status == "stopped":
            # Bỏ qua pod được ghim (pinned) cho VIP warmup
            if w.get("pinned_until", 0) > now:
                continue
            if idle_sec > settings.TERMINATE_TIMEOUT_SEC:
                # Pod đã stopped quá lâu → terminate hẳn
                logger.info(
                    f"[autoscale] TERMINATE stopped pod → {pod_id} "
                    f"(stopped {idle_sec}s > terminate_timeout {settings.TERMINATE_TIMEOUT_SEC}s)"
                )
                try:
                    await runpod.terminate_pod(pod_id)
                    await pool.remove(pod_id)
                except Exception as e:
                    logger.error(f"[autoscale] terminate stopped pod failed: {e}")


