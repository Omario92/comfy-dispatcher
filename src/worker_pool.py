import time
import json
from loguru import logger
from redis_client import get_redis
from config import settings


class WorkerPool:
    """
    Worker registry trong Redis HASH (workers:registry):
      field = pod_id
      value = JSON { pod_id, status, ip, port, last_active, current_job, worker_type }

    status: booting | idle | busy | dead
    worker_type: "image" | "video" | "any"
      - "image"  → chỉ nhận job output_type=image
      - "video"  → chỉ nhận job output_type=video
      - "any"    → nhận mọi loại job (backward-compatible, default)
    """

    async def mark_booting(self, pod_id: str, worker_type: str = "any"):
        """Ghi pod mới vào registry ngay lập tức với status=booting."""
        r = await get_redis()
        data = {
            "pod_id": pod_id,
            "status": "booting",
            "ip": "",
            "port": 0,
            "proxy_url": "",
            "last_active": int(time.time()),
            "current_job": None,
            "worker_type": worker_type,
        }
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))
        logger.info(f"[pool] marked {pod_id} as booting (worker_type={worker_type})")

    async def register_proxy(self, pod_id: str, proxy_url: str, worker_type: str = "any"):
        """Auto-register pod đang booting bằng RunPod proxy URL."""
        r = await get_redis()
        raw = await r.hget(settings.WORKERS_KEY, pod_id)
        data = json.loads(raw) if raw else {"pod_id": pod_id}
        data["status"] = "idle"
        data["proxy_url"] = proxy_url
        data["last_active"] = int(time.time())
        # Giữ worker_type nếu đã có từ mark_booting; override nếu truyền rõ ràng
        if worker_type != "any" or "worker_type" not in data:
            data["worker_type"] = worker_type
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))
        logger.info(f"[pool] auto-registered {pod_id} via proxy {proxy_url} (worker_type={data['worker_type']})")

    async def register(self, pod_id: str, ip: str, port: int, worker_type: str = "any"):
        r = await get_redis()
        data = {
            "pod_id": pod_id,
            "status": "idle",
            "ip": ip,
            "port": port,
            "last_active": int(time.time()),
            "current_job": None,
            "worker_type": worker_type,
        }
        await r.hset(settings.WORKERS_KEY, pod_id, json.dumps(data))
        logger.info(f"[pool] registered worker {pod_id} at {ip}:{port} (worker_type={worker_type})")

    async def list_workers(self) -> list[dict]:
        r = await get_redis()
        raw = await r.hgetall(settings.WORKERS_KEY)
        return [json.loads(v) for v in raw.values()]

    async def get_worker(self, pod_id: str) -> dict | None:
        r = await get_redis()
        raw = await r.hget(settings.WORKERS_KEY, pod_id)
        return json.loads(raw) if raw else None

    async def get_idle_worker(
        self,
        prefer_vip: bool = False,
        worker_type: str = "any",
    ) -> dict | None:
        """
        Trả về idle worker phù hợp với loại job.

        worker_type routing:
          - "image" / "video": ưu tiên pod cùng type → fallback pod "any"
          - "any": chọn bất kỳ pod idle (backward-compatible)

        prefer_vip=True (high-priority jobs): VIP pods trước → thường sau.
        prefer_vip=False (normal jobs): thường trước → VIP chỉ dùng khi không còn pod nào khác.
        """
        now = int(time.time())
        workers = await self.list_workers()
        idle = [w for w in workers if w["status"] == "idle"]

        # ── Lọc theo worker_type ──────────────────────────────────────
        if worker_type != "any":
            # Pod khớp đúng type hoặc pod "any" (backward-compat)
            typed   = [w for w in idle if w.get("worker_type", "any") == worker_type]
            generic = [w for w in idle if w.get("worker_type", "any") == "any"]
            # Ghép: đúng type trước, "any" pod là fallback
            candidates = typed + generic
        else:
            candidates = idle

        vip    = [w for w in candidates if w.get("pinned_until", 0) > now]
        normal = [w for w in candidates if w.get("pinned_until", 0) <= now]

        if prefer_vip:
            # High-priority: VIP first, then normal
            return (vip or normal or [None])[0]
        else:
            # Normal: avoid spending VIP pods, prefer normal workers
            return (normal or vip or [None])[0]

    async def count_idle_by_type(self) -> dict:
        """Đếm số pod idle theo worker_type. Dùng cho autoscaler smart scale-up."""
        workers = await self.list_workers()
        result = {"image": 0, "video": 0, "any": 0}
        for w in workers:
            if w.get("status") == "idle":
                wt = w.get("worker_type", "any")
                result[wt] = result.get(wt, 0) + 1
        return result

    async def count_active_by_type(self) -> dict:
        """Đếm số pod idle + booting theo worker_type.
        Dùng để autoscaler KHÔNG tạo thêm pod khi đã có pod đang boot."""
        workers = await self.list_workers()
        result = {"image": 0, "video": 0, "any": 0}
        for w in workers:
            if w.get("status") in ("idle", "booting"):
                wt = w.get("worker_type", "any")
                result[wt] = result.get(wt, 0) + 1
        return result

    async def mark_busy(self, pod_id: str, job_id: str):
        await self._update(pod_id, status="busy", current_job=job_id,
                          last_active=int(time.time()))

    async def mark_idle(self, pod_id: str):
        await self._update(pod_id, status="idle", current_job=None,
                          last_active=int(time.time()))

    async def mark_stopped(self, pod_id: str):
        """Pod đã bị stop (không có GPU, giữ /workspace). Có thể resume nhanh."""
        await self._update(pod_id, status="stopped", current_job=None)

    async def get_stopped_workers(self) -> list[dict]:
        """Lấy danh sách pod đang stopped (để resume khi có job mới)."""
        return [w for w in await self.list_workers() if w.get("status") == "stopped"]

    async def set_status(self, pod_id: str, status: str):
        await self._update(pod_id, status=status, last_active=int(time.time()))

    async def update_activity(self, pod_id: str):
        await self._update(pod_id, last_active=int(time.time()))

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
        out = {"idle": 0, "busy": 0, "booting": 0, "stopped": 0, "dead": 0, "total": len(workers)}
        for w in workers:
            s = w.get("status", "unknown")
            out[s] = out.get(s, 0) + 1
        return out


pool = WorkerPool()
