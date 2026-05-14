import json
import urllib.request
import os
import random
import uuid
import time

# Use PRODUCTION folder in project
WORKFLOW_DIR = "PRODUCTION"
TEST_IMAGE = "https://raw.githubusercontent.com/Omario92/comfyui-pod/main/test.jpg"

def main():
    if not os.path.exists(WORKFLOW_DIR):
        print(f"Directory {WORKFLOW_DIR} does not exist!")
        return

    files = [f for f in os.listdir(WORKFLOW_DIR) if f.endswith('.json')]
    if not files:
        print(f"No .json files found in {WORKFLOW_DIR}")
        return

    # Randomly pick a file
    chosen_file = random.choice(files)
    file_path = os.path.join(WORKFLOW_DIR, chosen_file)
    print(f"Selected workflow: {chosen_file}")

    with open(file_path, 'r', encoding='utf-8') as f:
        workflow = json.load(f)

    # Simulate node 413 injection
    if "413" not in workflow:
        workflow["413"] = {"inputs": {}, "class_type": "LoadImageFromHttpURL"}
    workflow["413"]["inputs"]["image_url"] = TEST_IMAGE

    # Hybrid payload to support both old and new versions during redeploy
    payload = {
        "job_id": f"test-{int(time.time())}", # old req
        "personality": 1,                      # old req (int)
        "user_image_url": TEST_IMAGE,          # old req
        "workflow": workflow,                  # both
        "image_url": TEST_IMAGE,               # new req
        "user_id": "test_agent",               # new opt
        "callback_url": ""                     # new opt
    }

    url = "https://comfy-dispatcher-production.up.railway.app/jobs"
    print(f"Sending request to {url}...")
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, 
        data=data, 
        headers={'Content-Type': 'application/json'}
    )

    try:
        with urllib.request.urlopen(req) as response:
            print("Response:", response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.read().decode('utf-8')}")
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
