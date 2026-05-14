import asyncio
import json
import sys

sys.path.append("src")
from config import settings
from redis_client import get_redis
from runpod_client import runpod

async def main():
    print("--- RUNPOD PODS ---")
    pods = await runpod.list_all_pods()
    for p in pods:
        print(f"{p['id']} - {p['name']} - {p.get('desiredStatus', 'UNKNOWN')}")
    
    print("\n--- REDIS WORKERS ---")
    r = await get_redis()
    w = await r.hgetall(settings.WORKERS_KEY)
    for k, v in w.items():
        data = json.loads(v)
        print(f"{k} - status: {data.get('status')} - active: {data.get('last_active')}")

asyncio.run(main())
