import asyncio
import json
import time
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger

from consumer import consumer_loop
from autoscaler import autoscale_loop
from health import health_loop
from worker_pool import pool
from job_store import jobs
from redis_client import get_redis, close_redis
from config import settings


# ============ LIFESPAN ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 50)
    logger.info("Dispatcher starting...")
    logger.info(f"MIN_WORKERS={settings.MIN_WORKERS} MAX_WORKERS={settings.MAX_WORKERS}")
    logger.info(f"SCALE_UP_THRESHOLD={settings.SCALE_UP_THRESHOLD}")
    logger.info(f"IDLE_TIMEOUT={settings.IDLE_TIMEOUT_SEC}s")
    logger.info("=" * 50)

    tasks = [
        asyncio.create_task(consumer_loop()),
        asyncio.create_task(autoscale_loop()),
        asyncio.create_task(health_loop()),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_redis()
        logger.info("Dispatcher stopped.")


app = FastAPI(lifespan=lifespan, title="Halida FaceSwap Dispatcher")


# ============ MODELS ============

class RegisterReq(BaseModel):
    pod_id: str
    ip: str
    port: int = 9000


class DoneReq(BaseModel):
    pod_id: str
    job_id: str
    result_url: str | None = None
    error: str | None = None


class SubmitJobReq(BaseModel):
    """Dùng cho trường hợp client (vd: n8n) push job qua HTTP thay vì LPUSH thẳng Redis."""
    job_id: str
    personality: int
    user_image_url: str
    workflow: dict


# ============ WORKER CALLBACKS ============

@app.post("/worker/register")
async def register_worker(req: RegisterReq):
    await pool.register(req.pod_id, req.ip, req.port)
    return {"ok": True}


@app.post("/worker/done")
async def worker_done(req: DoneReq):
    """Worker callback khi render xong (hoặc lỗi)."""
    logger.info(f"[done] pod={req.pod_id} job={req.job_id} error={req.error}")

    # Free worker
    await pool.mark_idle(req.pod_id)

    # Update job status
    job = await jobs.get(req.job_id)
    personality = int(job.get("personality", 0)) if job else 0

    if req.error:
        await jobs.set_failed(req.job_id, req.error)
    elif req.result_url:
        # img_personality sẽ được n8n callback workflow gắn vào;
        # ở đây dispatcher chỉ lưu result_url
        await jobs.set_done(req.job_id, req.result_url)

    # Forward sang n8n callback workflow (fire and forget với timeout ngắn)
    if settings.N8N_CALLBACK_URL:
        payload = {
            "job_id": req.job_id,
            "pod_id": req.pod_id,
            "personality": personality,
            "result_url": req.result_url,
            "error": req.error,
        }
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                resp = await c.post(settings.N8N_CALLBACK_URL, json=payload)
                if resp.status_code >= 400:
                    logger.warning(f"[done] n8n callback returned {resp.status_code}")
        except Exception as e:
            logger.error(f"[done] n8n callback failed: {e}")

    return {"ok": True}


# ============ JOB SUBMISSION (alternative to direct Redis push) ============

@app.post("/jobs")
async def submit_job(req: SubmitJobReq):
    """
    Endpoint để n8n submit job qua HTTP thay vì gọi Upstash REST trực tiếp.
    Đảm bảo job state được persist ngay khi enqueue.
    """
    await jobs.create(req.job_id, req.personality, req.user_image_url)
    r = await get_redis()
    await r.lpush(settings.QUEUE_KEY, json.dumps({
        "job_id": req.job_id,
        "personality": req.personality,
        "user_image_url": req.user_image_url,
        "workflow": req.workflow,
        "retries": 0,
    }))
    logger.info(f"[submit] enqueued {req.job_id} personality={req.personality}")
    return {"job_id": req.job_id, "status": "queued"}


# ============ STATUS / MONITORING ============

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    data = await jobs.get(job_id)
    if not data:
        raise HTTPException(404, "job not found")
    return data


@app.get("/health")
async def health():
    try:
        r = await get_redis()
        await r.ping()
        return {"status": "ok", "ts": int(time.time())}
    except Exception as e:
        raise HTTPException(503, f"redis down: {e}")


@app.get("/stats")
async def stats():
    r = await get_redis()
    counts = await pool.count_by_status()
    return {
        "queue_depth": await r.llen(settings.QUEUE_KEY),
        "workers": counts,
        "ts": int(time.time()),
    }


@app.get("/workers")
async def workers():
    return await pool.list_workers()


@app.post("/admin/scale-up")
async def admin_scale_up():
    """Manual scale up (debug)."""
    from autoscaler import scale_up
    result = await scale_up()
    return {"created": result}
