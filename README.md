# HALIDA Faceswap Dispatcher

Hệ thống **Dispatcher** bất đồng bộ cho dịch vụ AI Faceswap dựa trên ComfyUI + WAN 14B, chạy trên **Railway** và tự động quản lý GPU pod trên **RunPod**.

---

## 🏗 Kiến trúc tổng quan

```
WordPress/n8n
    │
    ▼  POST /jobs  (workflow JSON + callback_url)
┌──────────────────────────────────────────────────┐
│            DISPATCHER  (Railway - FastAPI)        │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌─────────────┐  │
│  │ job_store│   │ autoscaler│  │ worker_pool │  │
│  │ (Redis)  │   │ (loop)    │  │ (Redis)     │  │
│  └────┬─────┘   └─────┬────┘   └──────┬──────┘  │
│       │               │               │          │
│       └───────── job_processor ────────┘          │
│                       │                          │
└───────────────────────┼──────────────────────────┘
                        │ RunPod API
                        ▼
              ┌─────────────────────┐
              │   GPU Pod (RunPod)  │
              │  ComfyUI :8188      │
              │  WAN 14B model      │
              └──────────┬──────────┘
                         │ kết quả
                         ▼
                  Cloudflare R2
                         │
                         ▼
                  n8n Webhook → WordPress
```

---

## 📁 Cấu trúc project

```
HALIDA_Faceswap_Dispatcher/
│
├── src/                        # Core Dispatcher Service
│   ├── main.py                 # FastAPI app, toàn bộ HTTP endpoints
│   ├── config.py               # Cấu hình env variables (pydantic-settings)
│   ├── job_processor.py        # Pipeline xử lý job async (end-to-end)
│   ├── job_store.py            # CRUD job status vào Redis HASH
│   ├── worker_pool.py          # Registry pod (Redis HASH) + VIP priority dispatch
│   ├── autoscaler.py           # Auto scale-up/down pod theo queue
│   ├── comfy_client.py         # Giao tiếp ComfyUI: wait ready, submit, poll result
│   ├── runpod_client.py        # RunPod GraphQL API: create/stop/terminate/list pod
│   ├── r2_uploader.py          # Download output ComfyUI → upload Cloudflare R2
│   ├── health.py               # Health check loop, auto-register pod
│   ├── consumer.py             # Redis queue consumer (optional)
│   └── redis_client.py         # Redis connection singleton (Upstash TLS)
│
├── Front End/                  # Frontend tích hợp WordPress/Elementor
│   ├── PHP/
│   │   └── lh-faceswap-proxy.php       # PHP proxy: nhận request từ JS → Dispatcher
│   └── Elementor JS/
│       └── frontend_script.html        # JS widget Elementor: upload ảnh, gọi proxy, hiển thị kết quả
│
├── PRODUCTION/                 # ComfyUI workflow JSON sẵn sàng deploy
│   ├── HALIDA_FACESWAP_BDN_V2.json    # Workflow: Bà Đầm Nón
│   ├── HALIDA_FACESWAP_BTNG_V2.json   # Workflow: Bướm Trang Nghiêm
│   ├── HALIDA_FACESWAP_DCBN_V2.json   # Workflow: Dạ Cổ Buồn Nhớ
│   ├── HALIDA_FACESWAP_DSDM_V2.json   # Workflow: Duyên Sắc Đồng Màu
│   ├── HALIDA_FACESWAP_LPBN_V2.json   # Workflow: Lán Phán Bình Nho
│   └── HALIDA_FACESWAP_VTL_V2.json    # Workflow: Vũ Tiên Lữ
│
├── worker_agent.py             # FastAPI mini-agent chạy bên trong GPU Pod
├── Dockerfile                  # Build image cho Railway deployment
├── railway.toml                # Railway config (build + start command)
├── requirements.txt            # Python dependencies
├── .env.example                # Template biến môi trường
├── send_job.py                 # Script test gửi job thủ công (Python)
├── send_job.ps1                # Script test gửi job thủ công (PowerShell)
├── check_job.py                # Script kiểm tra trạng thái job
├── test_random_workflow.py     # Test workflow ngẫu nhiên
└── AGENTS.md                   # Quy tắc và lịch sử thay đổi (dev guidelines)
```

---

## ⚙️ Biến môi trường (`.env`)

| Biến | Bắt buộc | Mô tả |
|------|----------|-------|
| `REDIS_URL` | ✅ | Upstash Redis URL (rediss://...) |
| `RUNPOD_API_KEY` | ✅ | API Key RunPod |
| `RUNPOD_TEMPLATE_ID` | ✅ | ID template pod RunPod (chứa ComfyUI + WAN 14B) |
| `RUNPOD_GPU_TYPE` | ✅ | Danh sách GPU ưu tiên, phân tách bằng dấu phẩy |
| `RUNPOD_CLOUD_TYPE` | | `COMMUNITY` (test rẻ) hoặc `SECURE` (production) — mặc định `SECURE` |
| `RUNPOD_NETWORK_VOLUME_ID` | | ID Network Volume chứa model (tùy chọn) |
| `RUNPOD_MIN_CUDA_VERSION` | | Phiên bản CUDA tối thiểu — mặc định `12.8` |
| `N8N_CALLBACK_URL` | ✅ | Webhook n8n nhận kết quả khi job done |
| `DISPATCHER_PUBLIC_URL` | | URL public của Dispatcher (Railway) |
| `R2_ENDPOINT` | ✅ | Cloudflare R2 endpoint |
| `R2_BUCKET` | ✅ | Tên bucket R2 |
| `R2_ACCESS_KEY` | ✅ | R2 Access Key |
| `R2_SECRET_KEY` | ✅ | R2 Secret Key |
| `R2_PUBLIC_BASE` | ✅ | URL public R2 (`https://pub-xxx.r2.dev`) |
| `UPSTASH_REDIS_REST_URL` | | Upstash REST URL (cho n8n đọc trực tiếp) |
| `UPSTASH_REDIS_REST_TOKEN` | | Upstash REST Token |
| `IDLE_TIMEOUT_SEC` | | Thời gian idle trước khi stop pod — mặc định `120s` (test), nên đặt `600s` (production) |
| `PAUSE_TIMEOUT_SEC` | | Idle → podStop — mặc định `600s` |
| `TERMINATE_TIMEOUT_SEC` | | Idle → podTerminate — mặc định `1200s` |
| `MAX_WORKERS` | | Số pod tối đa — mặc định `10` |
| `COMFY_READY_TIMEOUT_SEC` | | Timeout chờ ComfyUI boot — mặc định `900s` (15 phút) |
| `COMFY_RESULT_TIMEOUT_SEC` | | Timeout chờ render — mặc định `1800s` (30 phút) |

---

## 🔄 Job Lifecycle

```
queued
  → starting_pod        (đang tìm / tạo pod)
  → waiting_comfyui     (đợi ComfyUI boot + load model ~8-12 phút)
  → running             (đã submit workflow, đang render)
  → done                (upload R2 xong, callback n8n)
  → failed              (lỗi bất kỳ bước nào)
```

Mỗi job được lưu trong Redis key `jobs:status:{job_id}` dưới dạng HASH với các field:
`job_id`, `status`, `pod_id`, `comfy_endpoint`, `comfy_prompt_id`, `result_url`, `error`, `personality`, `user_id`, `user_image_url`, `callback_url`, `created_at`, `updated_at`

---

## 🌐 API Endpoints

### Public

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/health` | Health check, trả trạng thái workers |
| `POST` | `/jobs` | Submit job mới — trả ngay `job_id` |
| `GET` | `/jobs/{job_id}` | Xem trạng thái job |

**Body `/jobs`:**
```json
{
  "personality": 1,
  "image_url": "https://...",
  "workflow": { ...ComfyUI workflow JSON... },
  "callback_url": "https://n8n.../webhook/...",
  "user_id": "wp_user_123"
}
```

### Worker (internal — gọi từ Pod)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `POST` | `/worker/register` | Pod tự đăng ký sau khi boot |
| `POST` | `/worker/heartbeat` | Pod gửi heartbeat định kỳ |

### Admin

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/admin/warmup-status` | Xem pod VIP đang được ghim |
| `POST` | `/admin/warmup` | Tạo N pod VIP GPU mạnh, ghim X giờ |
| `POST` | `/admin/warmup-cancel` | Hủy tất cả ghim VIP sớm |
| `POST` | `/admin/reconcile` | Đồng bộ Redis với RunPod thực tế (fix busy ảo) |
| `POST` | `/admin/cleanup-zombies` | Xóa pod zombie không nằm trong registry |
| `POST` | `/admin/terminate-pod` | Terminate pod cụ thể theo `podId` |
| `POST` | `/admin/job-recover` | Recover thủ công job bị stuck `running` |
| `POST` | `/admin/flush-workers` | Xóa toàn bộ registry (chỉ dùng khi debug) |

---

## 🚀 VIP Warmup Mode

Dùng khi chuẩn bị cho đợt traffic lớn (demo / ra mắt / giờ cao điểm):

```powershell
# 1. Kích hoạt — tạo 3 pod GPU cao cấp, ghim 4 giờ
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/warmup `
  -Method Post -ContentType "application/json" `
  -Body '{"count": 3, "duration_hours": 4}'

# 2. Kiểm tra trạng thái
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/warmup-status

# 3. Khi xong — giải phóng sớm để tiết kiệm chi phí
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/warmup-cancel -Method Post
```

**GPU ưu tiên VIP** (theo thứ tự): RTX PRO 6000 Blackwell → RTX 5090 → L40S → A100 80GB

- Pod VIP **không bao giờ bị autoscaler tắt** trong thời gian ghim
- Job mới **ưu tiên dispatch vào pod VIP** trước pod thường
- Nếu GPU hết slot (`SUPPLY_CONSTRAINT`) → báo lỗi rõ ràng, **không tự dùng GPU rẻ hơn**

---

## 🔧 Admin Recovery

### Fix busy ảo (pod stopped nhưng Redis vẫn báo busy)
```powershell
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/reconcile -Method Post
```

### Terminate pod cụ thể
```powershell
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/terminate-pod `
  -Method Post -ContentType "application/json" `
  -Body '{"podId": "abc123xyz"}'
```

### Recover job bị stuck `running`
```powershell
Invoke-RestMethod -Uri https://comfy-dispatcher-production.up.railway.app/admin/job-recover `
  -Method Post -ContentType "application/json" `
  -Body '{
    "job_id": "lhfs_xxxxx",
    "prompt_id": "<comfy_prompt_id từ Redis>",
    "comfy_endpoint": "https://<pod_id>-8188.proxy.runpod.net"
  }'
```

---

## 🐳 Deploy

### Railway (Dispatcher)
1. Fork repo → connect Railway
2. Set tất cả biến môi trường trên Railway Dashboard
3. Railway tự build từ `Dockerfile` và start bằng lệnh trong `railway.toml`

### RunPod Pod Template
- **Image**: `omaryo92/comfyui-deps:v2.0` (ComfyUI + WAN 14B + custom nodes)
- **Ports**: `8188/http` (ComfyUI), `9000/http` (Worker Agent)
- **Network Volume**: mount `/workspace` chứa models

---

## 📦 Dependencies chính

```
fastapi / uvicorn      — Web framework
httpx                  — Async HTTP client
redis                  — Upstash Redis client (TLS)
pydantic-settings      — Env config
tenacity               — Retry logic
boto3                  — Cloudflare R2 (S3-compatible)
loguru                 — Logging
```

---

## 📋 Lịch sử thay đổi

Xem file [AGENTS.md](./AGENTS.md) để biết chi tiết các thay đổi theo từng ngày.
