from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ===== Redis (Upstash TCP) =====
    REDIS_URL: str
    QUEUE_KEY: str = "faceswap_jobs"
    WORKERS_KEY: str = "workers:registry"
    JOB_STATUS_PREFIX: str = "jobs:status:"
    PROMPT_MAP_KEY: str = "prompt:pod_map"
    JOB_TTL_SEC: int = 86400  # 1 day

    # ===== RunPod =====
    RUNPOD_API_KEY: str
    RUNPOD_TEMPLATE_ID: str
    RUNPOD_GPU_TYPE: str = "NVIDIA GeForce RTX 5090,NVIDIA L40S"
    RUNPOD_API_URL: str = "https://api.runpod.io/graphql"
    RUNPOD_NETWORK_VOLUME_ID: str = ""
    # Chỉ lấy host có driver hỗ trợ CUDA >= version này
    # Image omaryo92/comfyui-deps:v2.0 yêu cầu CUDA >= 12.8
    RUNPOD_MIN_CUDA_VERSION: str = "12.8"
    # "COMMUNITY" (rẻ hơn, dùng khi test) hoặc "SECURE" (ổn định, production)
    RUNPOD_CLOUD_TYPE: str = "SECURE"
    COMFYUI_ARGS: str = ""  # Các custom arguments truyền vào cho ComfyUI khi start pod

    # ===== Autoscale =====
    MIN_WORKERS: int = 0  # 0 = scale-to-zero
    MAX_WORKERS: int = 10
    SCALE_UP_THRESHOLD: int = 10
    IDLE_TIMEOUT_SEC: int = 120         # testing default (2 phút)
    AUTOSCALE_INTERVAL_SEC: int = 10
    BOOT_TIMEOUT_SEC: int = 600  # 10 min

    # Peak hours (production) — set trong .env khi lên production
    # Giờ VN (UTC+7): 8h-22h là peak, ngoài ra là off-peak
    PEAK_HOURS_START: int | None = None  # VD: 8  (8:00 SA)
    PEAK_HOURS_END:   int | None = None  # VD: 22 (10:00 PM)
    PEAK_IDLE_TIMEOUT_SEC:     int = 900   # 15 phút trong peak
    OFF_PEAK_IDLE_TIMEOUT_SEC: int = 300   # 5 phút ngoài peak
    PEAK_MIN_WORKERS:          int = 1     # giữ ít nhất 1 GPU trong peak

    # 2-phase idle lifecycle:
    #   idle > PAUSE_TIMEOUT  → podStop  (giải phóng GPU, giữ /workspace)
    #   idle > TERMINATE_TIMEOUT → podTerminate (xóa hoàn toàn)
    # Testing defaults: 120s pause, 300s terminate
    # Production: nên đặt PAUSE_TIMEOUT = PEAK_IDLE_TIMEOUT, TERMINATE = PAUSE * 2
    PAUSE_TIMEOUT_SEC:     int = 600   # idle 10 phút mới stop pod
    TERMINATE_TIMEOUT_SEC: int = 1200  # idle 20 phút mới xóa hẳn

    # ===== Worker =====
    WORKER_AGENT_PORT: int = 9000  # FastAPI agent in pod
    WORKER_TIMEOUT_SEC: int = 600  # max render time
    HEALTH_CHECK_INTERVAL_SEC: int = 30

    # ===== ComfyUI Polling =====
    COMFY_READY_TIMEOUT_SEC: int = 900    # max wait for ComfyUI to boot (15 min — WAN 14B load từ /workspace)
    COMFY_RESULT_TIMEOUT_SEC: int = 1800  # max wait for render result (30 min)
    COMFY_POLL_INTERVAL_SEC: int = 5      # interval between history polls

    # ===== ComfyUI WebSocket (tối ưu latency) =====
    COMFY_WS_PING_INTERVAL: int = 20          # heartbeat ping mỗi N giây để giữ alive qua Cloudflare
    COMFY_WS_RECONNECT_MAX_ATTEMPTS: int = 5  # số lần retry khi WS mất kết nối

    # ===== URLs =====
    DISPATCHER_PUBLIC_URL: str = "" # https://comfy-dispatcher.up.railway.app

    # n8n callback khi job xong — dispatcher POST về đây
    # Dùng URL riêng theo output_type để n8n xử lý đúng mapping image/video
    N8N_CALLBACK_URL:      str = "" # fallback chung (legacy)
    N8N_IMG_CALLBACK_URL:  str = "" # https://n8n.../webhook/halida-faceswap-img-result
    N8N_VID_CALLBACK_URL:  str = "" # https://n8n.../webhook/halida-faceswap-video-result

    # ===== Cloudflare R2 =====
    R2_ENDPOINT: str = ""
    R2_BUCKET: str = "halida-faceswap"
    R2_ACCESS_KEY: str = ""
    R2_SECRET_KEY: str = ""
    R2_PUBLIC_BASE: str = ""

    # ===== Upstash Redis REST =====
    UPSTASH_REDIS_REST_URL: str = ""
    UPSTASH_REDIS_REST_TOKEN: str = ""


settings = Settings()
