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