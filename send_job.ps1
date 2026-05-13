$workflow_json = Get-Content -Raw "f:\App\HALIDA_Faceswap_Dispatcher\test_workflow.json"

$payload = @{
    job_id = "job-test-FULL"
    personality = 1
    user_image_url = "https://example.com/test.jpg"
    workflow = ($workflow_json | ConvertFrom-Json)
}

$jsonPayload = $payload | ConvertTo-Json -Depth 20

Invoke-RestMethod -Uri "https://comfy-dispatcher-production.up.railway.app/jobs" -Method Post -Headers @{"Content-Type"="application/json"} -Body $jsonPayload
