"""
RunPod Worker Agent for ComfyUI
-------------------------------
Hướng dẫn sử dụng trên RunPod:
1. Đặt file này vào Network Volume của bạn (ví dụ: /workspace/worker_agent.py)
2. Trong RunPod Template, đảm bảo mục "Exposed HTTP Ports" có chứa: 8188, 9000
3. Sửa mục "Docker Command" trong RunPod Template thành:
   bash -c "pip install fastapi uvicorn boto3 httpx && python /workspace/worker_agent.py & /start.sh"
   (Lưu ý: /start.sh là lệnh khởi động mặc định của ảnh runpod/comfyui:main. Nếu bạn dùng ảnh khác, hãy thay /start.sh bằng lệnh gốc của ảnh đó).
"""

import asyncio
import os
import json
import time
import httpx
import boto3
from fastapi import FastAPI, Request, BackgroundTasks
import uvicorn

app = FastAPI()

# ================= CẤU HÌNH =================
COMFY_URL = "http://127.0.0.1:8188"
DISPATCHER_URL = os.getenv("DISPATCHER_URL", "https://comfy-dispatcher-production.up.railway.app")

# Tự động lấy thông tin R2 & N8N từ môi trường (hoặc bạn có thể điền cứng vào đây)
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "ĐIỀN_ENDPOINT_CỦA_BẠN_NẾU_KHÔNG_DÙNG_ENV")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "ĐIỀN_KEY_CỦA_BẠN")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "ĐIỀN_SECRET_CỦA_BẠN")
R2_BUCKET = os.getenv("R2_BUCKET", "halida-faceswap")
R2_PUBLIC_BASE = os.getenv("R2_PUBLIC_BASE", "https://r2.halida.vn")

N8N_WEBHOOK = os.getenv("N8N_WEBHOOK", "ĐIỀN_LINK_N8N_CỦA_BẠN")
POD_ID = os.getenv("RUNPOD_POD_ID", "local")

# Khởi tạo S3 Client cho Cloudflare R2
s3 = None
if R2_ENDPOINT and "ĐIỀN" not in R2_ENDPOINT:
    s3 = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY
    )

async def wait_for_comfy():
    """Đợi ComfyUI khởi động xong."""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.get(f"{COMFY_URL}/queue", timeout=2.0)
                if r.status_code == 200:
                    print("[Agent] ComfyUI is ready!")
                    break
            except:
                pass
            await asyncio.sleep(2)

async def check_job_completion(prompt_id: str, job_id: str, personality: int):
    """Vòng lặp kiểm tra job kết thúc, lấy file, up R2 và báo n8n."""
    print(f"[Agent] Đang theo dõi tiến độ của prompt_id: {prompt_id}")
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
                r = await client.get(f"{COMFY_URL}/history/{prompt_id}")
                if r.status_code == 200:
                    data = r.json()
                    if prompt_id in data:
                        history = data[prompt_id]
                        # Job đã xong, tìm file output
                        outputs = history.get("outputs", {})
                        output_files = []
                        for node_id, node_output in outputs.items():
                            if "images" in node_output:
                                for img in node_output["images"]:
                                    output_files.append(img["filename"])
                            if "gifs" in node_output: 
                                for vid in node_output["gifs"]:
                                    output_files.append(vid["filename"])
                        
                        print(f"[Agent] Render xong. Output: {output_files}")
                        await process_outputs(job_id, output_files, personality)
                        return
            except Exception as e:
                print(f"[Agent] Lỗi khi check history: {e}")
            
            await asyncio.sleep(5)

async def process_outputs(job_id: str, output_filenames: list, personality: int):
    """Upload kết quả lên R2 và gửi Webhook về n8n."""
    results = []
    
    for filename in output_filenames:
        filepath = f"/workspace/ComfyUI/output/{filename}"
        if not os.path.exists(filepath):
            continue
            
        if s3:
            # Upload lên R2
            object_name = f"output/{job_id}/{filename}"
            print(f"[Agent] Đang upload {filename} lên R2...")
            try:
                # Chạy boto3 sync trong thread để không block asyncio
                await asyncio.to_thread(
                    s3.upload_file, filepath, R2_BUCKET, object_name
                )
                file_url = f"{R2_PUBLIC_BASE}/{object_name}"
                results.append(file_url)
                print(f"[Agent] Upload thành công: {file_url}")
            except Exception as e:
                print(f"[Agent] Lỗi upload R2: {e}")
        else:
            print("[Agent] R2 chưa được cấu hình. Bỏ qua upload.")
            results.append(filename)

    # Báo cho Dispatcher là worker này đã rảnh
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{DISPATCHER_URL}/worker/done",
                json={"pod_id": POD_ID, "job_id": job_id, "status": "completed", "result": {"urls": results}}
            )
        print(f"[Agent] Đã báo cáo hoàn thành cho Dispatcher.")
    except Exception as e:
        print(f"[Agent] Lỗi gọi /worker/done: {e}")

    # Gọi webhook về n8n
    if N8N_WEBHOOK and "ĐIỀN" not in N8N_WEBHOOK:
        payload = {
            "job_id": job_id,
            "personality": personality,
            "status": "completed",
            "result": {
                "files": results
            }
        }
        print(f"[Agent] Gửi webhook tới n8n: {payload}")
        try:
            async with httpx.AsyncClient() as client:
                await client.post(N8N_WEBHOOK, json=payload)
        except Exception as e:
            print(f"[Agent] Lỗi gửi webhook: {e}")

@app.post("/job")
async def receive_job(request: Request, background_tasks: BackgroundTasks):
    """Nhận job từ Dispatcher, chuyển cho ComfyUI và theo dõi."""
    payload = await request.json()
    job_id = payload.get("job_id")
    workflow = payload.get("workflow")
    personality = payload.get("personality", 1)

    print(f"[Agent] Nhận job mới: {job_id}")

    # Gửi workflow thẳng vào ComfyUI
    comfy_payload = {"prompt": workflow, "client_id": job_id}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{COMFY_URL}/prompt", json=comfy_payload)
        if r.status_code != 200:
            return {"status": "error", "message": "Failed to queue in ComfyUI"}
        
        data = r.json()
        prompt_id = data.get("prompt_id")

    # Bắt đầu background task để theo dõi tiến độ và upload
    background_tasks.add_task(check_job_completion, prompt_id, job_id, personality)

    return {"status": "queued", "prompt_id": prompt_id}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(wait_for_comfy())
    print(f"[Agent] Khởi động tại Pod: {POD_ID}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
