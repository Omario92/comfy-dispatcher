import asyncio
import json
import sys

sys.path.append("src")
from redis_client import get_redis
from config import settings

async def main():
    r = await get_redis()
    w = await r.hgetall(settings.WORKERS_KEY)
    print("WORKERS IN REDIS:")
    for k, v in w.items():
        print(f"{k} -> {json.dumps(json.loads(v), indent=2)}")

asyncio.run(main())
