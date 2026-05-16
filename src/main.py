import asyncio
import json
import time
import uuid
import httpx
from contextlib import asynccontextmanager
from typing import Literal, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger

from consumer import consumer_loop
from autoscaler import autoscale_loop
from health import health_loop
from worker_pool import pool
from job_store import jobs
from job_processor import process_job
from redis_client import get_redis, close_redis
from runpod_client import runpod
from config import settings


# ============ LIFESPAN ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 50)
    logger.info("Dispatcher starting...")
    logger.info(f"MIN_WORKERS={settings.MIN_WORKERS} MAX_WORKERS={settings.MAX_WORKERS}")
    logger.info(f"SCALE_UP_THRESHOLD={settings.SCALE_UP_THRESHOLD}")
    logger.info(f"IDLE_TIMEOUT={settings.IDLE_TIMEOUT_SEC}s")
    logger.info("=" * 50)

    tasks = [
        asyncio.create_task(consumer_loop()),
        asyncio.create_task(autoscale_loop()),
        asyncio.create_task(health_loop()),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_redis()
        logger.info("Dispatcher stopped.")


app = FastAPI(lifespan=lifespan, title="Halida FaceSwap Dispatcher")


# ============ MODELS ============

class RegisterReq(BaseModel):
    pod_id: str
    ip: str
    port: int = 9000


class DoneReq(BaseModel):
    pod_id: str
    job_id: str
    result_url: str | None = None
    error: str | None = None


class SubmitJobReq(BaseModel):
    """
    Backward-compatible: chấp nhận cả field cũ của n8n (user_image_url, job_id)
    lẫn field mới (image_url). Dispatcher luôn tự sinh job_id riêng.
    """
    model_config = {"extra": "allow"}   # bỏ qua job_id và field lạ từ n8n

    workflow: dict                       # Full ComfyUI JSON (đã inject image_url)

    # Chấp nhận cả 2 tên field — ít nhất 1 trong 2 phải có
    image_url:      str = ""            # field mới
    user_image_url: str = ""            # field cũ của n8n

    personality: str | int = ""
    user_id: str = ""
    job_id: str = ""
    callback_url: str = ""
    priority: Literal["high", "normal"] = "normal"
    output_type: Literal["image", "video"] = "video"
    job_label: Optional[str] = None

    @property
    def resolved_image_url(self) -> str:
        """Lấy image URL từ field mới hoặc field cũ, validate không rỗng."""
        url = self.image_url or self.user_image_url
        return url.strip()


# ============ ADMIN ============

@app.post("/admin/job-recover")
async def job_recover(body: dict):
    """
    Recover thủ công job bị stuck 'running' khi ComfyUI đã xong nhưng Dispatcher không nhận được.
    Cần truyền: job_id + prompt_id (lấy từ Redis) + comfy_endpoint (proxy URL của pod).
    """
    from comfy_client import build_view_url, extract_output_files, pick_primary_output
    from r2_uploader import download_and_upload_r2
    from job_processor import _callback_n8n, _guess_content_type

    job_id   = body.get("job_id", "")
    prompt_id = body.get("prompt_id", "")
    comfy_endpoint = body.get("comfy_endpoint", "")

    if not all([job_id, prompt_id, comfy_endpoint]):
        raise HTTPException(status_code=400, detail="Required: job_id, prompt_id, comfy_endpoint")

    headers = {"Authorization": f"Bearer {settings.RUNPOD_API_KEY}"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{comfy_endpoint}/history/{prompt_id}", headers=headers)
            r.raise_for_status()
            history = r.json()

        if prompt_id not in history:
            raise HTTPException(status_code=404, detail=f"prompt_id {prompt_id} not found in ComfyUI history")

        history_item = history[prompt_id]
        files = extract_output_files(history_item)
        output_file = pick_primary_output(files)

        if not output_file:
            raise HTTPException(status_code=404, detail="No output file in history")

        view_url = build_view_url(comfy_endpoint, output_file)
        ext = output_file["filename"].rsplit(".", 1)[-1] if "." in output_file["filename"] else "mp4"
        r2_key = f"outputs/{job_id}/{output_file['filename']}"
        result_url = await download_and_upload_r2(view_url, r2_key, _guess_content_type(ext))

        job_data = await jobs.get(job_id)
        personality = (job_data or {}).get("personality", "")
        await jobs.set_done(job_id, result_url, img_personality=str(personality))

        pod_id = (job_data or {}).get("pod_id", "")
        await _callback_n8n(job_id, "done", result_url, None, pod_id, prompt_id)
        if pod_id:
            await pool.mark_idle(pod_id)

        logger.info(f"[admin] ✅ job-recover: job={job_id} → {result_url}")
        return {"status": "recovered", "job_id": job_id, "result_url": result_url}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[admin] job-recover failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class RegisterPodReq(BaseModel):
    pod_id: str
    proxy_url: str = ""   # https://{pod_id}-8188.proxy.runpod.net  (để trống nếu dùng IP)
    ip: str = ""          # Public IP nếu dùng SECURE cloud (thay vì proxy)
    port: int = 8188      # Port ComfyUI (mặc định 8188)
    pin_hours: float = 0  # > 0 để ghim pod, autoscaler sẽ bỏ qua nó
    worker_type: str = "any"  # "image" | "video" | "any" — routing model-affinity


@app.post("/admin/register-pod")
async def admin_register_pod(req: RegisterPodReq):
    """
    Đăng ký thủ công một Pod đã được deploy sẵn trên RunPod web vào Redis Dispatcher.

    Dùng khi Admin tự bật Pod trên giao diện web RunPod và muốn Dispatcher
    nhận job và dispatch thẳng vào Pod đó.

    Cách lấy proxy_url:
      - Vào RunPod → Pod details → tab "Connect"
      - Chọn port 8188 → copy "Connect to HTTP Service"
      - Dạng: https://{pod_id}-8188.proxy.runpod.net

    Body JSON:
      {
        "pod_id":    "abc123xyz",
        "proxy_url": "https://abc123xyz-8188.proxy.runpod.net",
        "pin_hours": 4     (tuỳ chọn — ghim pod N giờ, autoscaler bỏ qua)
      }
    """
    if not req.pod_id:
        raise HTTPException(status_code=400, detail="pod_id is required")
    if not req.proxy_url and not req.ip:
        raise HTTPException(
            status_code=400,
            detail="Cần ít nhất 1 trong 2: proxy_url (Community Cloud) hoặc ip (Secure Cloud)"
        )

    # Kiểm tra pod đã tồn tại chưa
    existing = await pool.get_worker(req.pod_id)

    if req.proxy_url:
        # Community Cloud: dùng RunPod Proxy URL
        await pool.register_proxy(req.pod_id, req.proxy_url, worker_type=req.worker_type)
    else:
        # Secure Cloud: dùng IP trực tiếp
        await pool.register(req.pod_id, req.ip, req.port, worker_type=req.worker_type)

    # Ghim pod nếu admin muốn
    pin_msg = None
    if req.pin_hours > 0:
        pinned_until = int(time.time()) + int(req.pin_hours * 3600)
        await pool._update(req.pod_id, pinned_until=pinned_until)
        pin_msg = time.strftime("%H:%M:%S %d/%m/%Y", time.localtime(pinned_until))

    action = "updated" if existing else "registered"
    logger.info(
        f"[admin] manually {action} pod {req.pod_id} "
        f"proxy={req.proxy_url or '-'} ip={req.ip or '-'} "
        f"worker_type={req.worker_type} "
        f"pinned_until={pin_msg or 'no pin'}"
    )

    return {
        "status": "ok",
        "action": action,
        "pod_id": req.pod_id,
        "proxy_url": req.proxy_url or None,
        "ip": req.ip or None,
        "port": req.port,
        "worker_type": req.worker_type,
        "pinned_until": pin_msg,
        "note": "Pod đã được đăng ký vào Redis. Dispatcher sẽ dispatch job vào Pod này ngay khi có job mới."
    }


@app.post("/admin/terminate-all")
async def terminate_all_pods():
    """Xóa sạch toàn bộ Pod trong registry và trên RunPod."""
    workers = await pool.list_workers()
    terminated = []
    errors = []
    
    for w in workers:
        pod_id = w["pod_id"]
        try:
            await runpod.terminate_pod(pod_id)
            await pool.remove(pod_id)
            terminated.append(pod_id)
        except Exception as e:
            errors.append({"pod_id": pod_id, "error": str(e)})
            
    logger.warning(f"[admin] terminate-all: {len(terminated)} killed, {len(errors)} failed")
    return {
        "status": "complete",
        "terminated_count": len(terminated),
        "terminated_ids": terminated,
        "failed_count": len(errors),
        "errors": errors
    }


@app.post("/admin/flush-workers")
async def flush_workers():
    """Xóa toàn bộ stale workers khỏi Redis registry (dùng khi debug)."""
    workers = await pool.list_workers()
    for w in workers:
        await pool.remove(w["pod_id"])
    logger.warning(f"[admin] flushed {len(workers)} workers from registry")
    return {"flushed": len(workers)}


class WarmupReq(BaseModel):
    count: int = 3
    duration_hours: float = 4.0
    cloud_type: str = "SECURE"
    worker_type: str = "any"   # "image" | "video" | "any" — gán type cho các pod VIP được tạo
    # Chỉ GPU cao cấp — không tự fallback xuống card rẻ hơn
    gpu_types: list[str] = [
        "NVIDIA RTX PRO 6000 Blackwell Server Edition",
        "NVIDIA RTX PRO 6000 Blackwell Workstation Edition",
        "NVIDIA GeForce RTX 5090",
        "NVIDIA L40S",
    ]


@app.post("/admin/terminate-pod")
async def terminate_pod_by_id(body: dict):
    """Terminate một pod cụ thể theo pod_id."""
    pod_id = body.get("podId") or body.get("pod_id")
    if not pod_id:
        raise HTTPException(status_code=400, detail="podId required")
    try:
        await runpod.terminate_pod(pod_id)
        await pool.remove(pod_id)
        logger.info(f"[admin] manually terminated pod {pod_id}")
        return {"terminated": pod_id}
    except LookupError:
        await pool.remove(pod_id)
        return {"terminated": pod_id, "note": "already gone from RunPod"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/pause-pod")
async def pause_pod_by_id(body: dict):
    """
    Tạm dừng (Stop) một pod cụ thể theo pod_id để tiết kiệm chi phí GPU.
    Pod sẽ được giữ lại ổ cứng (/workspace) và trạng thái trong Redis chuyển thành 'stopped'.
    Nếu pod này là VIP, nó vẫn giữ nguyên trạng thái 'pinned' và sẽ được tự động resume khi có request mới.
    """
    pod_id = body.get("pod_id") or body.get("podId")
    if not pod_id:
        raise HTTPException(status_code=400, detail="pod_id required")
    try:
        await runpod.stop_pod(pod_id)
        await pool.mark_stopped(pod_id)
        logger.info(f"[admin] manually paused pod {pod_id}")
        return {"status": "paused", "pod_id": pod_id}
    except LookupError:
        await pool.remove(pod_id)
        return {"status": "not_found", "pod_id": pod_id, "note": "already gone from RunPod"}
    except Exception as e:
        logger.exception(f"[admin] pause-pod failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/warmup")
async def warmup_vip(req: WarmupReq):
    """
    Tạo N pod VIP với GPU mạnh và ghim chúng trong X giờ.
    Autoscaler sẽ KHÔNG tắt các pod này trong thời gian được ghim.
    Dùng trước khi mở đợt traffic lớn (demo / ra mắt / giờ cao điểm).
    """
    pinned_until = int(time.time()) + int(req.duration_hours * 3600)
    expires_at = time.strftime("%H:%M:%S %d/%m/%Y", time.localtime(pinned_until))

    original_gpu = settings.RUNPOD_GPU_TYPE
    original_cloud = settings.RUNPOD_CLOUD_TYPE
    settings.RUNPOD_GPU_TYPE = ",".join(req.gpu_types)
    settings.RUNPOD_CLOUD_TYPE = req.cloud_type.upper()

    created = []
    failed = []
    MAX_RETRIES = 5

    for i in range(req.count):
        success = False
        last_err = "unknown"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                pod = await runpod.create_pod(f"comfy-vip-{uuid.uuid4().hex[:8]}")
                pod_id = pod["id"]
                await pool.mark_booting(pod_id, worker_type=req.worker_type)
                await pool._update(pod_id, pinned_until=pinned_until)
                logger.info(f"[warmup] 🔒 pinned VIP pod {pod_id} until {expires_at} (worker_type={req.worker_type})")
                created.append(pod_id)
                success = True
                break
            except Exception as e:
                cause = getattr(e, "last_attempt", None)
                last_err = str(cause.exception()) if cause else str(e)
                # Nếu lỗi SUPPLY_CONSTRAINT thì không retry — GPU hết slot
                if "SUPPLY_CONSTRAINT" in last_err:
                    logger.warning(f"[warmup] pod #{i+1} attempt {attempt}: GPU supply exhausted")
                    break
                logger.warning(f"[warmup] pod #{i+1} attempt {attempt}/{MAX_RETRIES}: {last_err}")
                await asyncio.sleep(2)

        if not success:
            logger.error(f"[warmup] pod #{i+1} FAILED after {MAX_RETRIES} attempts: {last_err}")
            failed.append(f"Pod #{i+1}: {last_err}")

    settings.RUNPOD_GPU_TYPE = original_gpu
    settings.RUNPOD_CLOUD_TYPE = original_cloud

    return {
        "status": "warmup_initiated",
        "created": len(created),
        "failed": len(failed),
        "pod_ids": created,
        "worker_type": req.worker_type,
        "pinned_until": expires_at,
        "duration_hours": req.duration_hours,
        "errors": failed,
    }


@app.get("/admin/warmup-status")
async def warmup_status():
    """Xem danh sách các pod VIP đang được ghim và thời gian còn lại."""
    now = int(time.time())
    workers = await pool.list_workers()
    pinned = []
    for w in workers:
        pin = w.get("pinned_until", 0)
        if pin > now:
            remaining_min = (pin - now) // 60
            pinned.append({
                "pod_id": w["pod_id"],
                "status": w.get("status"),
                "pinned_until": time.strftime("%H:%M:%S %d/%m/%Y", time.localtime(pin)),
                "remaining_minutes": remaining_min,
            })
    return {"total_pinned": len(pinned), "pinned_pods": pinned}


@app.post("/admin/warmup-cancel")
async def warmup_cancel():
    """Giải phóng tất cả pin VIP sớm (autoscaler sẽ scale down bình thường)."""
    workers = await pool.list_workers()
    released = []
    for w in workers:
        if w.get("pinned_until", 0) > 0:
            await pool._update(w["pod_id"], pinned_until=0)
            released.append(w["pod_id"])
    return {"released": len(released), "pod_ids": released}


@app.post("/admin/cleanup-zombies")
async def cleanup_zombies():
    """
    Quét RunPod và xóa mọi Pod 'comfy-worker-' không có trong registry.
    Dùng để dọn dẹp khi Dispatcher mất đồng bộ với RunPod.
    """
    try:
        all_runpod_pods = await runpod.list_all_pods()
        registered_workers = await pool.list_workers()
        registered_ids = {w["pod_id"] for w in registered_workers}

        terminated = []
        skipped = []
        for p in all_runpod_pods:
            pod_id = p.get("id", "")
            pod_name = p.get("name", "")
            if not pod_id:
                continue

            # Chỉ xóa những pod do Dispatcher tạo (theo prefix name) nhưng không có trong registry
            if pod_name.startswith("comfy-worker-") and pod_id not in registered_ids:
                logger.warning(f"[admin] terminating zombie pod: {pod_name} ({pod_id})")
                try:
                    await runpod.terminate_pod(pod_id)
                    terminated.append(pod_id)
                except LookupError:
                    # Pod đã bị xóa rồi
                    terminated.append(pod_id)
            else:
                skipped.append({"id": pod_id, "name": pod_name})

        return {
            "status": "cleanup_complete",
            "total_runpod_pods": len(all_runpod_pods),
            "registered_in_dispatcher": len(registered_ids),
            "terminated_count": len(terminated),
            "terminated_ids": terminated,
            "skipped": skipped,
        }
    except Exception as e:
        logger.exception(f"[admin] cleanup-zombies failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/reconcile")
async def reconcile_workers():
    """
    Đồng bộ trạng thái Redis với RunPod thực tế.
    Pod nào đang 'busy'/'booting' trong Redis nhưng không còn RUNNING trên RunPod
    → reset về 'idle' để giải phóng slot.
    """
    try:
        all_runpod_pods = await runpod.list_all_pods()
        # Map pod_id → desiredStatus từ RunPod
        runpod_status = {p["id"]: p.get("desiredStatus", "UNKNOWN") for p in all_runpod_pods}

        workers = await pool.list_workers()
        fixed = []
        removed = []

        for w in workers:
            pod_id = w["pod_id"]
            redis_status = w.get("status", "unknown")
            rp_state = runpod_status.get(pod_id, None)

            if rp_state is None:
                # Pod không tồn tại trên RunPod → xóa khỏi registry
                logger.warning(f"[reconcile] pod {pod_id} not found on RunPod → removing")
                await pool.remove(pod_id)
                removed.append(pod_id)

            elif rp_state in ("EXITED", "STOPPED") and redis_status in ("busy", "booting", "idle"):
                # Pod đã dừng nhưng Redis vẫn đánh dấu busy/booting/idle
                # → mark_stopped (KHÔNG phải idle) để autoscaler quyết định resume hay terminate
                logger.warning(f"[reconcile] pod {pod_id} is {rp_state} on RunPod but '{redis_status}' in Redis → mark stopped")
                await pool.mark_stopped(pod_id)
                fixed.append({"pod_id": pod_id, "was": redis_status, "runpod_state": rp_state, "now": "stopped"})

            elif redis_status == "dead":
                # Pod bị health_loop mark là dead do mất kết nối quá lâu
                logger.warning(f"[reconcile] pod {pod_id} is 'dead' in Redis → removing")
                await pool.remove(pod_id)
                removed.append(pod_id)

        return {
            "status": "reconcile_complete",
            "checked": len(workers),
            "removed_ghost_pods": removed,
            "reset_to_idle": fixed,
        }
    except Exception as e:
        logger.exception(f"[admin] reconcile failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/fix-busy")
async def fix_busy_pods():
    """
    Tìm những pod đang bị đánh dấu là 'busy' nhưng thực tế không có job nào đang chạy
    (hoặc job đã xong/lỗi) và reset chúng về 'idle'.
    """
    workers = await pool.list_workers()
    fixed = []
    for w in workers:
        if w.get("status") == "busy":
            pod_id = w["pod_id"]
            current_job = w.get("current_job")
            
            # Nếu pod không có job_id gán kèm, chắc chắn là lỗi state
            if not current_job:
                await pool.mark_idle(pod_id)
                fixed.append({"pod_id": pod_id, "reason": "no_current_job"})
                continue
                
            # Kiểm tra trạng thái job
            job_data = await jobs.get(current_job)
            if not job_data or job_data.get("status") in ("done", "failed"):
                await pool.mark_idle(pod_id)
                fixed.append({
                    "pod_id": pod_id, 
                    "reason": f"job_{job_data.get('status') if job_data else 'not_found'}"
                })

    return {"status": "success", "reset_to_idle": fixed}


@app.post("/admin/check-comfy-exec")
async def check_comfy_exec(body: dict):
    """
    Dùng podExec (trực tiếp trên Pod, bỏ qua Proxy) để kiểm tra /history của ComfyUI.
    Cần truyền: pod_id, prompt_id.
    Lưu ý: podExec KHÔNG hoạt động trên Community Cloud.
    """
    pod_id = body.get("pod_id")
    prompt_id = body.get("prompt_id")
    if not pod_id or not prompt_id:
        raise HTTPException(status_code=400, detail="Required: pod_id, prompt_id")

    # Gọi curl http://127.0.0.1:8188/history/{prompt_id} bên trong pod
    cmd = f"curl -s http://127.0.0.1:8188/history/{prompt_id}"
    logger.info(f"[admin] check-comfy-exec pod_id={pod_id} cmd={cmd}")
    
    result = await runpod.execute_command(pod_id, cmd)
    if result.get("exitCode") != 0:
        return {"status": "error", "message": "curl failed inside pod", "exec_result": result}
        
    try:
        import json
        stdout = result.get("stdout", "")
        # RunPod podExec đôi khi trả về string có ký tự escape hoặc không phải JSON sạch
        # Cố gắng parse
        history = json.loads(stdout)
        is_done = prompt_id in history
        return {
            "status": "success", 
            "is_done": is_done, 
            "history_data": history
        }
    except Exception as e:
        return {"status": "parse_error", "message": str(e), "stdout": result.get("stdout")}


# ============ WORKER CALLBACKS ============

@app.post("/worker/register")
async def register_worker(req: RegisterReq):
    await pool.register(req.pod_id, req.ip, req.port)
    return {"ok": True}


@app.post("/worker/done")
async def worker_done(req: DoneReq):
    """Worker callback khi render xong (hoặc lỗi)."""
    logger.info(f"[done] pod={req.pod_id} job={req.job_id} error={req.error}")

    # Free worker
    await pool.mark_idle(req.pod_id)

    # Update job status
    job = await jobs.get(req.job_id)
    personality = int(job.get("personality", 0)) if job else 0

    if req.error:
        await jobs.set_failed(req.job_id, req.error)
    elif req.result_url:
        # img_personality sẽ được n8n callback workflow gắn vào;
        # ở đây dispatcher chỉ lưu result_url
        await jobs.set_done(req.job_id, req.result_url)

    # Forward sang n8n callback workflow (fire and forget với timeout ngắn)
    if settings.N8N_CALLBACK_URL:
        payload = {
            "job_id": req.job_id,
            "pod_id": req.pod_id,
            "personality": personality,
            "result_url": req.result_url,
            "error": req.error,
        }
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                resp = await c.post(settings.N8N_CALLBACK_URL, json=payload)
                if resp.status_code >= 400:
                    logger.warning(f"[done] n8n callback returned {resp.status_code}")
        except Exception as e:
            logger.error(f"[done] n8n callback failed: {e}")

    return {"ok": True}


# ============ JOB SUBMISSION ============

@app.post("/jobs")
async def submit_job(req: SubmitJobReq):
    """
    n8n gọi endpoint này sau khi đã:
      1. Random chọn workflow (0-5 personality)
      2. Download workflow JSON từ Google Drive
      3. Inject image_url vào node 413 (LoadImageFromHttpURL)

    Dispatcher:
      - Tự sinh job_id
      - Lưu job vào Redis ngay lập tức
      - Trả { ok, job_id, status } ngay về n8n (không chờ render)
      - Chạy full pipeline (pod → ComfyUI → R2 → callback) trong background
    """
    # Ưu tiên dùng job_id từ n8n/PHP gửi lên (lhfs_xxx), nếu không có mới tự sinh
    job_id = req.job_id or f"job_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

    image_url = req.resolved_image_url
    if not image_url:
        raise HTTPException(400, "image_url (or user_image_url) is required")

    await jobs.create(
        job_id=job_id,
        personality=req.personality,
        user_image_url=image_url,
        workflow=req.workflow,
        callback_url=req.callback_url,
        user_id=req.user_id,
        priority=req.priority,
        output_type=req.output_type,
        job_label=req.job_label or "",
    )

    # Fire-and-forget — KHÔNG await, n8n nhận job_id ngay lập tức
    asyncio.create_task(process_job(job_id))

    logger.info(
        f"[submit] job_id={job_id} personality={req.personality} "
        f"priority={req.priority} output_type={req.output_type} "
        f"user_id={req.user_id} callback={bool(req.callback_url)} "
        f"image={image_url[:60]}"
    )
    return {"ok": True, "job_id": job_id, "status": "queued"}


# ============ STATUS / MONITORING ============

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    data = await jobs.get(job_id)
    if not data:
        raise HTTPException(404, "job not found")
    return data


@app.get("/health")
async def health():
    try:
        r = await get_redis()
        await r.ping()
        return {"status": "ok", "ts": int(time.time())}
    except Exception as e:
        raise HTTPException(503, f"redis down: {e}")


@app.get("/stats")
async def stats():
    r = await get_redis()
    counts = await pool.count_by_status()
    return {
        "queue_depth": await r.llen(settings.QUEUE_KEY),
        "workers": counts,
        "ts": int(time.time()),
    }


@app.get("/workers")
async def workers():
    return await pool.list_workers()


@app.post("/admin/scale-up")
async def admin_scale_up(body: dict = {}):
    """Manual scale up (debug). Truyền worker_type để tạo đúng loại pod."""
    from autoscaler import scale_up
    worker_type = body.get("worker_type", "any") if body else "any"
    result = await scale_up(worker_type=worker_type)
    return {"created": result, "worker_type": worker_type}
