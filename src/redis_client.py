import redis.asyncio as redis
from config import settings

_pool: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    global _pool
    if _pool is None:
        # Railway injects env vars without stripping quotes — strip defensively
        url = settings.REDIS_URL.strip().strip('"').strip("'")
        _pool = redis.from_url(
            url,
            decode_responses=True,
            max_connections=20,
            socket_connect_timeout=5,
            socket_keepalive=True,
        )
    return _pool


async def close_redis():
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None