# HALIDA_Faceswap_Dispatcher Rules & Guidelines

## Rules
1. Maintain clean architecture and strict separation of concerns.
2. Adhere to environment variables strictly through `config.py`.
3. Dispatcher must remain highly available, all long running tasks must be asynchronous.
4. Keep logs formatted properly using `loguru`.
5. Sau khi hoàn thành task lớn, phải cập nhật phần "Recent Changes".

## Commands
- `/push-code`: Push current changes to git repository.

## Recent Changes
- [2026-05-12] Setup Dispatcher Service trên Railway với FastAPI, Redis và RunPod API integration (Giai đoạn 6).
- [2026-05-13] Refactor async job flow: POST /jobs tự sinh job_id và trả ngay về n8n, background pipeline (job_processor.py) xử lý pod → ComfyUI → R2 → callback. Thêm comfy_client.py (submit/poll ComfyUI), r2_uploader.py (upload boto3). n8n chỉ cần gửi full workflow JSON đã inject image_url + callback_url.
- [2026-05-13] Bugfix series: (1) health loop không được kill busy pod đang render; (2) mark pod busy ngay lập tức sau khi acquire để tránh race condition với autoscaler; (3) health loop không được reset last_active của idle pod — lỗi này khiến autoscaler idle timeout không bao giờ trigger; (4) thêm R2 credential validation rõ ràng.
- [2026-05-13] Stability & Integration Fixes: (1) Fix JSON parse error (frontend) bằng cách unwrap array response từ n8n; (2) Fix PHP Proxy cURL output leak gây hỏng JSON response; (3) Thêm fail-fast logic phát hiện ComfyUI crash (404); (4) Tự động xóa Ghost Pods (POD_NOT_FOUND) khỏi registry; (5) Tăng COMFY_READY_TIMEOUT lên 900s cho các model lớn như WAN 14B.
- [2026-05-13] Infrastructure Resilience: (1) Triển khai podExec (TCL) để gửi lệnh trực tiếp vào Pod, bypass hoàn toàn lỗi Proxy 404 của RunPod; (2) Thêm Admin Endpoint `/admin/cleanup-zombies` để tự động dọn dẹp Pod thừa (Zombie Pods); (3) Tăng Idle Timeout lên 10 phút để tránh việc Autoscaler tắt Pod quá sớm khi đang load model.
- [2026-05-13] VIP Warmup Mode: (1) `/admin/warmup` tạo N pod GPU cao cấp (RTX PRO 6000/RTX 5090/L40S/A100) và ghim chúng X giờ; (2) Autoscaler bỏ qua pod được ghim; (3) Job mới ưu tiên vào pod VIP trước; (4) `/admin/reconcile` đồng bộ Redis với RunPod thực tế; (5) `/admin/terminate-pod` xóa pod cụ thể; (6) Disable podExec (400 trên Community Cloud).
- [2026-05-14] Manual Pod Registration: Thêm `POST /admin/register-pod` để Admin gán thủ công Pod ID (đã deploy trên RunPod web) vào Upstash Redis. Hỗ trợ cả Community Cloud (proxy_url) và Secure Cloud (ip+port). Tùy chọn `pin_hours` để ghim pod, autoscaler bỏ qua.
- [2026-05-15] WebSocket Optimization: Thay polling 5s bằng WebSocket real-time cho ComfyUI (latency giảm từ ~5s → real-time). Implement `AsyncComfyWebSocketClient` (persistent connection, heartbeat, auto-reconnect, exponential backoff). Thêm `wait_for_result()` = WS + tự động fallback polling cũ nếu WS fail (zero-downtime). Thêm `websockets==13.1` vào requirements. Thêm `COMFY_WS_PING_INTERVAL` và `COMFY_WS_RECONNECT_MAX_ATTEMPTS` vào config. job_processor.py chỉ thay 1 dòng `poll_result` → `wait_for_result`.
- [2026-05-15] Deploy Customization: Thêm tùy chọn `COMFYUI_ARGS` vào `config.py` (và map sang biến môi trường `env` khi gọi RunPod API) để hỗ trợ truyền các arguments tuỳ chỉnh cho ComfyUI khi khởi tạo pod mới.
- [2026-05-15] Booting Fail-Fast: Cập nhật `health.py` tự động query RunPod API cho các pod đang ở trạng thái `booting`. Nếu phát hiện `desiredStatus == "EXITED"` (do lỗi container exit status 1 hoặc context deadline exceeded), dispatcher sẽ lập tức terminate pod thay vì chờ hết `BOOT_TIMEOUT_SEC`.
- [2026-05-15] GPU Compatibility: Loại bỏ `NVIDIA A100 80GB PCIe` khỏi danh sách GPU hỗ trợ do không tương thích với Sage Attention build trên Docker image hiện tại.
- [2026-05-15] Manual Worker Deployment: Deployed a manual worker pod `gobheagkjto88v` (SECURE Cloud) using `scratch/deploy_worker.py` to handle immediate rendering demand.
- [2026-05-15] Bugfix: Fixed `COMFYUI_ARGS` quoting issue that caused ComfyUI to fail to boot (removed quotes in `.env` and added `.strip('"')` in deployment logic).
- [2026-05-16] Created `CLAUDE.md` as a copy of `AGENTS.md` for better compatibility with different AI agents.
- [2026-05-16] Dual-Job Parallel FaceSwap (v2.0): Triển khai hệ thống song song 2 luồng — Image (priority: high) + Video (priority: normal). Chi tiết:
  - **src/main.py**: Thêm 3 field vào `SubmitJobReq`: `priority: Literal["high","normal"]`, `output_type: Literal["image","video"]`, `job_label: Optional[str]`. Endpoint `/jobs` log và pass 3 field xuống `jobs.create()`.
  - **src/job_store.py**: `create()` nhận và lưu `priority`, `output_type`, `job_label` vào Redis HASH.
  - **src/worker_pool.py**: `get_idle_worker(prefer_vip=False)` — `prefer_vip=True` (high-priority): chọn VIP pod trước; `prefer_vip=False` (normal): tránh VIP pod, tiết kiệm cho image jobs.
  - **src/job_processor.py**: Đọc priority ngay khi start; gọi `_acquire_worker(prefer_vip=True)` cho high-priority jobs; `_callback_n8n` payload bổ sung `output_type` và `job_label`.
  - **lh-faceswap-proxy.php** (v2.0): `/swap` tạo 2 job ID riêng (`lhfs_img_xxx`, `lhfs_vid_xxx`), lưu 2 transient, trả cả 2 ID về frontend; `/result` xử lý `output_type` — image lưu `image_url`/`preview_url`, video lưu `video_url`; `/status` chấp nhận mọi prefix `lhfs_`.
  - **frontend_script.html** (v2.0): State thêm `imageJobId`, `videoJobId`, `imageResult`, `videoUrl`; `pollJobResult()` generic; sau `/swap` poll image (3s) song song với video (5s background); `showQuickImageResult()` chuyển Step-06 ngay khi image xong; `preloadVideoInBackground()` load video ẩn + enable `#btn-get-video`; click `#btn-get-video` → hiện video section + autoplay + scroll; `resetUploadForm()` reset đầy đủ state mới; CSS mới cho `#result-image`, `.video-section-hidden`, `#btn-get-video` pulse, `#video-status-hint`.
- [2026-05-16] Bugfix Critical: n8n callback 404 khiến frontend poll mãi không nhận được kết quả. Root cause: webhook `halida-faceswap-img-result` / `halida-faceswap-video-result` trên n8n trả 404 → WordPress transient không bao giờ được update → frontend thấy `status: pending` mãi. Fix (v2.1): Endpoint PHP `/status` giờ tự **fallback query thẳng Dispatcher API** (`GET /jobs/{job_id}`) khi transient còn `pending`. Nếu Dispatcher báo `done`, PHP tự update transient và trả kết quả về frontend ngay — bypass hoàn toàn n8n callback. Thêm `DISPATCHER_URL` và `DISPATCHER_API_KEY` config vào PHP proxy. Plugin version → 2.1.
- [2026-05-16] Worker Type Routing (v3.0): Phân loại pod theo `worker_type: "image" | "video" | "any"` để tránh ComfyUI unload/reload model khi job image và video được dispatch lộn xộn. Chi tiết:
  - **src/worker_pool.py**: Thêm `worker_type` vào `mark_booting()`, `register()`, `register_proxy()`. `get_idle_worker(worker_type=...)` ưu tiên pod cùng type → fallback pod "any". Thêm `count_idle_by_type()` cho autoscaler.
  - **src/autoscaler.py**: `scale_up(worker_type)` đặt tên pod `image-worker-xxx` / `video-worker-xxx` / `comfy-worker-xxx`. `_tick()` đọc 2 Redis counter `queue:image_pending` / `queue:video_pending` → scale đúng loại pod khi thiếu. Resume stopped pod giữ nguyên `worker_type`.
  - **src/job_processor.py**: Tăng pending counter khi job start, giảm khi xong (cả lỗi). `_acquire_worker(output_type=...)` truyền type vào `get_idle_worker` + `scale_up`. Log rõ worker nào được acquire cho job nào.
  - **src/main.py**: `RegisterPodReq` thêm `worker_type`. `/admin/register-pod`, `/admin/warmup`, `/admin/scale-up` đều nhận và truyền `worker_type`.
  - **src/config.py**: Thêm `IMAGE_PENDING_KEY` và `VIDEO_PENDING_KEY` (Redis counter keys).
  - **Backward-compat**: Pod cũ không có `worker_type` được treat là `"any"` → nhận mọi loại job.
- [2026-05-16] Autoscaler & Job Routing Fixes:
  - **Bugfix (4 pods per job)**: Loại bỏ duplicate `scale_up` gọi từ cả `autoscaler.py` và `job_processor.py`. Sửa logic `_acquire_worker` kiểm tra active pod theo đúng `worker_type` thay vì kiểm tra tổng, giúp video jobs không bị block khi image pods đang boot.
  - **PHP Personality Map**: Thêm `PERSONALITY_IMG_MAP` vào `lh-faceswap-proxy.php` để map số personality (0-5) từ Dispatcher sang URL ảnh thật khi dùng fallback `dispatcher_direct` (bypass n8n).
  - **Mobile Upload Resilience**: Fix lỗi đứng/treo khi upload trên mobile bằng cách (1) Hỗ trợ file type HEIC rỗng trên iOS Safari; (2) Thêm timeout 8s cho `canvas.toBlob()` tránh treo RAM; (3) Thêm `AbortController` timeout (70s/30s) cho các fetch request tránh treo do mạng yếu.
  - **UI Optimization**: Chuẩn hóa breakpoints (Tablet: 1024px, Mobile: 767px) trên toàn bộ frontend; Tăng kích thước kính lúp trên mobile thêm 30%; Vô hiệu hóa site footer của theme bằng CSS.






## vexp <!-- vexp v2.0.12 -->

**MANDATORY: use `run_pipeline` — do NOT grep or glob the codebase.**
vexp returns pre-indexed, graph-ranked context in a single call.

### Workflow
1. `run_pipeline` with your task description — ALWAYS FIRST (replaces all other tools)
2. Make targeted changes based on the context returned
3. `run_pipeline` again only if you need more context

### Available MCP tools
- `run_pipeline` — **PRIMARY TOOL**. Runs capsule + impact + memory in 1 call.
  Auto-detects intent. Includes file content. Example: `run_pipeline({ "task": "fix auth bug" })`
- `get_skeleton` — compact file structure
- `index_status` — indexing status
- `expand_vexp_ref` — expand V-REF placeholders in v2 output

### Agentic search
- Do NOT use built-in file search, grep, or codebase indexing — always call `run_pipeline` first
- If you spawn sub-agents or background tasks, pass them the context from `run_pipeline`
  rather than letting them search the codebase independently

### Smart Features
Intent auto-detection, hybrid ranking, session memory, auto-expanding budget.

### Multi-Repo
`run_pipeline` auto-queries all indexed repos. Use `repos: ["alias"]` to scope. Run `index_status` to see aliases.
<!-- /vexp -->