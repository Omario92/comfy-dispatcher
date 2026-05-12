import asyncio
import time
import httpx
from loguru import logger
from worker_pool import pool
from runpod_client import runpod
from config import settings


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
        ok = await _ping(w)
        if ok:
            continue

        age = int(time.time()) - w.get("last_active", 0)
        # Cho 10 phút boot, sau đó coi như dead
        if age > settings.BOOT_TIMEOUT_SEC:
            logger.warning(f"[health] worker {w['pod_id']} unreachable for {age}s, killing")
            try:
                await runpod.terminate_pod(w["pod_id"])
            except Exception:
                pass
            await pool.remove(w["pod_id"])


async def _ping(worker: dict) -> bool:
    """Ping agent endpoint /health."""
    url = f"http://{worker['ip']}:{worker['port']}/health"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(url)
            return r.status_code == 200
    except Exception:
        return False
