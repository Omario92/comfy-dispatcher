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

        age = int(time.time()) - w.get("last_active", 0)

        # KHÔNG bao giờ terminate pod đang busy (đang render)
        if status == "busy":
            # Chỉ update last_active cho busy pod khi còn reachable
            if ok:
                await pool.update_activity(pod_id)
            
            # --- Auto Recovery & Phantom Fix ---
            current_job = w.get("current_job")
            if not current_job:
                logger.warning(f"[health] Pod {pod_id} is busy but has no current_job. Force idle.")
                await pool.mark_idle(pod_id)
            else:
                from job_store import JobStore
                jobs_store = JobStore()
                job_data = await jobs_store.get(current_job)
                
                if not job_data or job_data.get("status") in ("done", "failed"):
                    logger.warning(f"[health] Pod {pod_id} is busy but job {current_job} is done/failed/missing. Force idle.")
                    await pool.mark_idle(pod_id)
                elif job_data.get("status") in ("running", "processing", "waiting_comfyui"):
                    updated_at = int(job_data.get("updated_at", 0))
                    job_age = int(time.time()) - updated_at
                    
                    if job_age > 300: # 5 phút
                        # Tránh spam log liên tục mỗi 30s bằng cách chỉ log 1 lần mỗi 2.5 phút
                        if job_age % 150 < 30: 
                            logger.warning(f"[health] Job {current_job} on pod {pod_id} stuck for {job_age}s. Auto-triggering recovery...")
                        
                        import httpx
                        import os
                        import asyncio
                        async def _trigger_recover():
                            port = os.getenv("PORT", "8000")
                            try:
                                async with httpx.AsyncClient(timeout=10) as client:
                                    # Lặng lẽ gọi endpoint để thử check history
                                    await client.post(f"http://127.0.0.1:{port}/admin/job-recover", json={"job_id": current_job})
                            except Exception:
                                pass # Nếu 404 (chưa xong) thì kệ, chờ cycle sau
                        
                        asyncio.create_task(_trigger_recover())
            
            if age > settings.BOOT_TIMEOUT_SEC * 3:  # 30 phút grace cho render
                logger.warning(
                    f"[health] busy pod {pod_id} unreachable for {age}s "
                    f"(> {settings.BOOT_TIMEOUT_SEC * 3}s), marking dead"
                )
                await pool.set_status(pod_id, "dead")
            continue

        # idle pod: KHÔNG update last_active — để autoscaler đo đúng thời gian idle
        # Chỉ kill nếu pod không reachable quá lâu (BOOT_TIMEOUT_SEC)
        if not ok and age > settings.BOOT_TIMEOUT_SEC:
            logger.warning(f"[health] idle worker {pod_id} unreachable for {age}s, killing")
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
            # Chỉ coi là OK khi ComfyUI thực sự trả về 200
            # 404 từ RunPod proxy = pod không chạy hoặc route sai
            return r.status_code == 200
    except Exception:
        return False
