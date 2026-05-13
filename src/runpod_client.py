import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger
from config import settings


class RunPodClient:
    """Wrapper cho RunPod GraphQL API."""

    def __init__(self):
        self.url = settings.RUNPOD_API_URL
        self.headers = {
            "Authorization": f"Bearer {settings.RUNPOD_API_KEY}",
            "Content-Type": "application/json",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def create_pod(self, name: str) -> dict:
        query = """
        mutation CreatePod($input: PodFindAndDeployOnDemandInput!) {
          podFindAndDeployOnDemand(input: $input) {
            id
            desiredStatus
            machineId
          }
        }
        """
        gpu_types = [g.strip() for g in settings.RUNPOD_GPU_TYPE.split(",") if g.strip()]
        last_error = "No valid GPU types provided"
        
        async with httpx.AsyncClient(timeout=30) as client:
            for cloud_type in ["SECURE", "COMMUNITY"]:
                for gpu in gpu_types:
                    variables = {
                        "input": {
                            "cloudType": cloud_type,
                            "gpuCount": 1,
                            "volumeInGb": 0,
                            "containerDiskInGb": 40,
                            "gpuTypeId": gpu,
                            "name": name,
                            "templateId": settings.RUNPOD_TEMPLATE_ID,
                            "ports": f"{settings.WORKER_AGENT_PORT}/http,8188/http",
                            "minCudaVersion": settings.RUNPOD_MIN_CUDA_VERSION,
                        }
                    }
                    if settings.RUNPOD_NETWORK_VOLUME_ID:
                        variables["input"]["networkVolumeId"] = settings.RUNPOD_NETWORK_VOLUME_ID
                    r = await client.post(
                        self.url,
                        json={"query": query, "variables": variables},
                        headers=self.headers,
                    )
                    r.raise_for_status()
                    data = r.json()
                    
                    if "errors" in data:
                        last_error = str(data["errors"])
                        logger.warning(f"RunPod failed for {gpu} ({cloud_type}): {last_error}")
                        continue
                    
                    # Success
                    logger.info(f"Successfully created pod with {gpu} in {cloud_type} cloud")
                    return data["data"]["podFindAndDeployOnDemand"]
                    
        raise Exception(f"All GPU fallback attempts failed. Last error: {last_error}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def stop_pod(self, pod_id: str) -> bool:
        """Stop pod (giải phóng GPU nhưng giữ /workspace volume). Không tính tiền GPU khi stopped."""
        query = """
        mutation StopPod($input: PodStopInput!) {
          podStop(input: $input) {
            id
            desiredStatus
          }
        }
        """
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                self.url,
                json={"query": query, "variables": {"input": {"podId": pod_id}}},
                headers=self.headers,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                # Nếu pod không tồn tại → raise để autoscaler có thể remove khỏi registry
                codes = [e.get("extensions", {}).get("code", "") for e in data["errors"]]
                if "POD_NOT_FOUND" in codes:
                    raise LookupError(f"POD_NOT_FOUND: {pod_id}")
                logger.error(f"RunPod stop error: {data['errors']}")
                return False
            logger.info(f"[runpod] stopped pod {pod_id}")
            return True

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def resume_pod(self, pod_id: str, gpu_count: int = 1) -> bool:
        """Resume a stopped pod (restart GPU)."""
        query = """
        mutation ResumePod($input: PodResumeInput!) {
          podResume(input: $input) {
            id
            desiredStatus
          }
        }
        """
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                self.url,
                json={"query": query, "variables": {"input": {"podId": pod_id, "gpuCount": gpu_count}}},
                headers=self.headers,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                logger.error(f"RunPod resume error: {data['errors']}")
                return False
            logger.info(f"[runpod] resumed pod {pod_id}")
            return True

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def terminate_pod(self, pod_id: str) -> bool:
        query = """
        mutation TerminatePod($input: PodTerminateInput!) {
          podTerminate(input: $input)
        }
        """
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                self.url,
                json={"query": query, "variables": {"input": {"podId": pod_id}}},
                headers=self.headers,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                # Nếu pod không tồn tại → raise để autoscaler có thể remove khỏi registry
                codes = [e.get("extensions", {}).get("code", "") for e in data["errors"]]
                if "POD_NOT_FOUND" in codes:
                    raise LookupError(f"POD_NOT_FOUND: {pod_id}")
                logger.error(f"RunPod terminate error: {data['errors']}")
                return False
            return True

    async def get_pod(self, pod_id: str) -> dict | None:
        query = """
        query Pod($podId: String!) {
          pod(input: {podId: $podId}) {
            id
            desiredStatus
            runtime {
              ports { ip publicPort privatePort isIpPublic }
            }
          }
        }
        """
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                self.url,
                json={"query": query, "variables": {"podId": pod_id}},
                headers=self.headers,
            )
            return r.json().get("data", {}).get("pod")


runpod = RunPodClient()
