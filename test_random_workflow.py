import json
import urllib.request
import os
import random
import uuid

WORKFLOW_DIR = r"k:\My Drive\HALIDA LANDING PAGE 2026\WORKFLOW_JSON\PRODUCTION"

def main():
    # Tìm tất cả file .json trong thư mục PRODUCTION
    files = [f for f in os.listdir(WORKFLOW_DIR) if f.endswith('.json')]
    if not files:
        print(f"Không tìm thấy file .json nào trong {WORKFLOW_DIR}")
        return

    # Random 1 file
    chosen_file = random.choice(files)
    file_path = os.path.join(WORKFLOW_DIR, chosen_file)
    print(f"Đã chọn ngẫu nhiên workflow: {chosen_file}")

    # Đọc workflow
    with open(file_path, 'r', encoding='utf-8') as f:
        workflow = json.load(f)

    # Tạo payload
    job_id = f"job-test-random-{uuid.uuid4().hex[:8]}"
    payload = {
        "job_id": job_id,
        "personality": 1,
        "user_image_url": "https://example.com/test.jpg",
        "workflow": workflow
    }

    # Gửi request
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        "https://comfy-dispatcher-production.up.railway.app/jobs", 
        data=data, 
        headers={'Content-Type': 'application/json'}
    )

    try:
        print(f"Đang gửi job {job_id}...")
        with urllib.request.urlopen(req) as response:
            print("Kết quả:", response.read().decode('utf-8'))
    except Exception as e:
        print("Lỗi khi gửi:", e)

if __name__ == "__main__":
    main()
