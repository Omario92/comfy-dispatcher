import time
import json
from loguru import logger
from redis_client import get_redis
from config import settings


class WorkerPool:
    """
    Worker registry trong Redis HASH (workers:registry):
      field = pod_id
      value = JSON { pod_id, status, ip, port, last_active, current_job }

    status: booting | idle | busy | dead
    """

    async def mark_booting(self, pod_id: str):
        """Ghi pod mới vào registry ngay lập tức với status=booting."""
        r = await get_redis()
        data = {
            "pod_id": pod_id,
            "status": "booting",
            "ip": "",
            "port": 0,
            "last_active": int(time.time()),
            "current_job": None,
        }
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))
        logger.info(f"[pool] marked {pod_id} as booting")

    async def register(self, pod_id: str, ip: str, port: int):
        r = await get_redis()
        data = {
            "pod_id": pod_id,
            "status": "idle",
            "ip": ip,
            "port": port,
            "last_active": int(time.time()),
            "current_job": None,
        }
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))
        logger.info(f"[pool] registered worker {pod_id} at {ip}:{port}")

    async def list_workers(self) -> list[dict]:
        r = await get_redis()
        raw = await r.hgetall(settings.WORKERS_KEY)
        return [json.loads(v) for v in raw.values()]

    async def get_worker(self, pod_id: str) -> dict | None:
        r = await get_redis()
        raw = await r.hget(settings.WORKERS_KEY, pod_id)
        return json.loads(raw) if raw else None

    async def get_idle_worker(self) -> dict | None:
        for w in await self.list_workers():
            if w["status"] == "idle":
                return w
        return None

    async def mark_busy(self, pod_id: str, job_id: str):
        await self._update(pod_id, status="busy", current_job=job_id,
                          last_active=int(time.time()))

    async def mark_idle(self, pod_id: str):
        await self._update(pod_id, status="idle", current_job=None,
                          last_active=int(time.time()))

    async def remove(self, pod_id: str):
        r = await get_redis()
        await r.hdel(settings.WORKERS_KEY, pod_id)
        logger.info(f"[pool] removed worker {pod_id}")

    async def _update(self, pod_id: str, **fields):
        r = await get_redis()
        raw = await r.hget(settings.WORKERS_KEY, pod_id)
        if not raw:
            return
        data = json.loads(raw)
        data.update(fields)
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))

    async def count_by_status(self) -> dict:
        workers = await self.list_workers()
        out = {"idle": 0, "busy": 0, "booting": 0, "dead": 0, "total": len(workers)}
        for w in workers:
            s = w.get("status", "unknown")
            out[s] = out.get(s, 0) + 1
        return out


pool = WorkerPool()
