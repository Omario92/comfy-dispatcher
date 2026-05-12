import time
from loguru import logger
from redis_client import get_redis
from config import settings


class JobStore:
    """Quản lý job status trong Redis (HASH per job)."""

    def _key(self, job_id: str) -> str:
        return f"{settings.JOB_STATUS_PREFIX}{job_id}"

    async def create(self, job_id: str, personality: int, user_image_url: str):
        r = await get_redis()
        key = self._key(job_id)
        await r.hset(key, mapping={
            "job_id": job_id,
            "status": "queued",
            "personality": personality,
            "user_image_url": user_image_url,
            "created_at": int(time.time()),
            "updated_at": int(time.time()),
        })
        await r.expire(key, settings.JOB_TTL_SEC)

    async def set_processing(self, job_id: str, pod_id: str):
        r = await get_redis()
        key = self._key(job_id)
        await r.hset(key, mapping={
            "status": "processing",
            "pod_id": pod_id,
            "updated_at": int(time.time()),
        })

    async def set_done(self, job_id: str, result_url: str, img_personality: str = ""):
        r = await get_redis()
        key = self._key(job_id)
        await r.hset(key, mapping={
            "status": "done",
            "result_url": result_url,
            "img_personality": img_personality,
            "updated_at": int(time.time()),
        })

    async def set_failed(self, job_id: str, error: str):
        r = await get_redis()
        key = self._key(job_id)
        await r.hset(key, mapping={
            "status": "failed",
            "error": error,
            "updated_at": int(time.time()),
        })

    async def get(self, job_id: str) -> dict | None:
        r = await get_redis()
        data = await r.hgetall(self._key(job_id))
        return data if data else None


jobs = JobStore()
