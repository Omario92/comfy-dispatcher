import time, urllib.request, json

job_id = 'job_1778639158142_0b6c13bd'
base_url = 'https://comfy-dispatcher-production.up.railway.app'
keys = ['status', 'error', 'pod_id', 'comfy_prompt_id', 'result_url']

start = time.time()
while True:
    time.sleep(30)
    elapsed = int(time.time() - start)
    try:
        res = urllib.request.urlopen(f'{base_url}/jobs/{job_id}').read().decode()
        d = json.loads(res)
        status = d.get('status', '')
        prompt = d.get('comfy_prompt_id', '')[:8] + '...' if d.get('comfy_prompt_id') else ''
        result = d.get('result_url', '')
        error = d.get('error', '')
        print(f"[{elapsed:>4}s] status={status:20s} prompt={prompt} result={result[:60]} {('ERR:'+error[:50]) if error else ''}")
        if status in ('done', 'failed'):
            break
    except Exception as e:
        print(f"[{elapsed:>4}s] poll error: {e}")
