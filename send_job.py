import json
import urllib.request

with open('test_workflow.json', 'r', encoding='utf-8') as f:
    workflow = json.load(f)

payload = {
    "job_id": "job-test-FULL",
    "personality": 1,
    "user_image_url": "https://example.com/test.jpg",
    "workflow": workflow
}

data = json.dumps(payload).encode('utf-8')
req = urllib.request.Request("https://comfy-dispatcher-production.up.railway.app/jobs", data=data, headers={'Content-Type': 'application/json'})

try:
    with urllib.request.urlopen(req) as response:
        print(response.read().decode('utf-8'))
except Exception as e:
    print(e)
