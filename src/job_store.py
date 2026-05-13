import time
from loguru import logger
from redis_client import get_redis
from config import settings


class JobStore:
    """Quản lý job status trong Redis (HASH per job)."""

    def _key(self, job_id: str) -> str:
        return f"{settings.JOB_STATUS_PREFIX}{job_id}"

    async def create(self, job_id: str, personality: int | str, user_image_url: str,
                      workflow: dict | None = None,
                      callback_url: str = "",
                      user_id: str = ""):
        import json
        r = await get_redis()
        key = self._key(job_id)
        mapping = {
            "job_id":        job_id,
            "status":        "queued",
            "personality":   str(personality),
            "user_image_url": user_image_url,
            "callback_url":  callback_url,
            "user_id":       user_id,
            "pod_id":        "",
            "comfy_endpoint": "",
            "comfy_prompt_id": "",
            "result_url":    "",
            "error":         "",
            "created_at":    int(time.time()),
            "updated_at":    int(time.time()),
        }
        if workflow is not None:
            mapping["workflow"] = json.dumps(workflow)
        await r.hset(key, mapping=mapping)
        await r.expire(key, settings.JOB_TTL_SEC)

    async def set_processing(self, job_id: str, pod_id: str):
        r = await get_redis()
        key = self._key(job_id)
        await r.hset(key, mapping={
            "status":     "processing",
            "pod_id":     pod_id,
            "updated_at": int(time.time()),
        })

    async def set_status(self, job_id: str, status: str):
        r = await get_redis()
        await r.hset(self._key(job_id), mapping={
            "status":     status,
            "updated_at": int(time.time()),
        })

    async def set_waiting_comfy(self, job_id: str, pod_id: str, comfy_endpoint: str):
        r = await get_redis()
        await r.hset(self._key(job_id), mapping={
            "status":         "waiting_comfyui",
            "pod_id":         pod_id,
            "comfy_endpoint": comfy_endpoint,
            "updated_at":     int(time.time()),
        })

    async def update_prompt_id(self, job_id: str, prompt_id: str):
        r = await get_redis()
        await r.hset(self._key(job_id), mapping={
            "comfy_prompt_id": prompt_id,
            "updated_at":      int(time.time()),
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
