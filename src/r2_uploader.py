"""
r2_uploader.py — Download output file từ ComfyUI proxy, upload lên Cloudflare R2.

Dùng boto3 (S3-compatible API).
Download chạy qua httpx async (stream), upload chạy trong thread pool (boto3 là sync).
"""
import asyncio

import boto3
import httpx
from loguru import logger

from config import settings


# ─────────────────────────── S3 client factory ───────────────────────────

def _make_s3():
    """Tạo boto3 S3 client trỏ vào Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT,
        aws_access_key_id=settings.R2_ACCESS_KEY,
        aws_secret_access_key=settings.R2_SECRET_KEY,
        region_name="auto",
    )


def _upload_sync(data: bytes, key: str, content_type: str) -> None:
    """Sync upload (chạy trong thread pool)."""
    s3 = _make_s3()
    s3.put_object(
        Bucket=settings.R2_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    logger.info(f"[r2] ✅ uploaded {len(data):,} bytes → s3://{settings.R2_BUCKET}/{key}")


# ─────────────────────────── Public API ───────────────────────────

async def download_and_upload_r2(
    comfy_view_url: str,
    r2_key: str,
    content_type: str = "video/mp4",
) -> str:
    """
    1. Download file từ ComfyUI /view (qua RunPod proxy, cần Bearer token).
    2. Upload lên Cloudflare R2.
    3. Trả về public URL.

    Timeout download 10 phút để xử lý file video lớn.
    """
    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}

    logger.info(f"[r2] downloading from ComfyUI: {comfy_view_url}")
    async with httpx.AsyncClient(timeout=600) as client:  # 10 min
        r = await client.get(comfy_view_url, headers=headers, follow_redirects=True)

    if r.status_code != 200:
        raise RuntimeError(
            f"Failed to download ComfyUI output: HTTP {r.status_code} — {r.text[:200]}"
        )

    data = r.content
    logger.info(f"[r2] downloaded {len(data):,} bytes, uploading to R2 key={r2_key}")

    # boto3 là sync → chạy trong thread pool để không block event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _upload_sync, data, r2_key, content_type)

    public_url = f"{settings.R2_PUBLIC_BASE.rstrip('/')}/{r2_key}"
    logger.info(f"[r2] public URL: {public_url}")
    return public_url
