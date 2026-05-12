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
    RUNPOD_GPU_TYPE: str = "RTX4090,RTX5090,RTX Pro 4500, L40S"
    RUNPOD_API_URL: str = "https://api.runpod.io/graphql"
    RUNPOD_NETWORK_VOLUME_ID: str = ""

    # ===== Autoscale =====
    MIN_WORKERS: int = 1
    MAX_WORKERS: int = 10
    SCALE_UP_THRESHOLD: int = 10
    IDLE_TIMEOUT_SEC: int = 900  # 15 min
    AUTOSCALE_INTERVAL_SEC: int = 10
    BOOT_TIMEOUT_SEC: int = 600  # 10 min

    # ===== Worker =====
    WORKER_AGENT_PORT: int = 9000  # FastAPI agent in pod
    WORKER_TIMEOUT_SEC: int = 600  # max render time
    HEALTH_CHECK_INTERVAL_SEC: int = 30

    # ===== URLs =====
    DISPATCHER_PUBLIC_URL: str = "" # https://comfy-dispatcher.up.railway.app
    N8N_CALLBACK_URL: str = ""      # https://n8n.../webhook/faceswap-done

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
