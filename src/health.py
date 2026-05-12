import asyncio
import time
import httpx
from loguru import logger
from worker_pool import pool
from runpod_client import runpod
from config import settings

# RunPod proxy URL format: https://{pod_id}-{port}.proxy.runpod.net
COMFYUI_PORT = 8188


def _proxy_url(pod_id: str, port: int = COMFYUI_PORT) -> str:
    return f"https://{pod_id}-{port}.proxy.runpod.net"


async def health_loop():
    logger.info("[health] started")
    while True:
        try:
            await _check_all()
        except asyncio.CancelledError:
            logger.info("[health] stopped")
            return
        except Exception as e:
            logger.exception(f"[health] check failed: {e}")
        await asyncio.sleep(settings.HEALTH_CHECK_INTERVAL_SEC)


async def _check_all():
    workers = await pool.list_workers()
    for w in workers:
        pod_id = w["pod_id"]
        status = w.get("status", "booting")

        if status == "booting":
            # Kiểm tra xem ComfyUI đã ready qua RunPod proxy chưa
            proxy = _proxy_url(pod_id)
            age = int(time.time()) - w.get("last_active", 0)
            if age < 60:
                # Chưa đủ 60 giây, bỏ qua
                continue

            ok = await _ping_url(f"{proxy}/queue")
            if ok:
                # ComfyUI đã lên, tự đăng ký worker với proxy URL
                logger.info(f"[health] auto-registering booting pod {pod_id} via proxy")
                await pool.register_proxy(pod_id, proxy)
            elif age > settings.BOOT_TIMEOUT_SEC:
                logger.warning(f"[health] pod {pod_id} boot timeout ({age}s), killing")
                try:
                    await runpod.terminate_pod(pod_id)
                except Exception:
                    pass
                await pool.remove(pod_id)
            continue

        # Với worker đã idle/busy: ping proxy hoặc direct IP
        ok = await _ping(w)
        if ok:
            continue

        age = int(time.time()) - w.get("last_active", 0)
        if age > settings.BOOT_TIMEOUT_SEC:
            logger.warning(f"[health] worker {pod_id} unreachable for {age}s, killing")
            try:
                await runpod.terminate_pod(pod_id)
            except Exception:
                pass
            await pool.remove(pod_id)


async def _ping(worker: dict) -> bool:
    """Ping worker: dùng proxy_url nếu có, nếu không dùng direct IP."""
    proxy_url = worker.get("proxy_url")
    if proxy_url:
        return await _ping_url(f"{proxy_url}/queue")
    ip = worker.get("ip", "")
    port = worker.get("port", 0)
    if not ip or not port:
        return False
    return await _ping_url(f"http://{ip}:{port}/health")


async def _ping_url(url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url)
            return r.status_code < 500
    except Exception:
        return False
