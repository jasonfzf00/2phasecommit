import requests
import json

# Try calling rpc method with timeout of 3
def rpc_call(server, method, params=None, timeout = 3):
    headers = {"Content-Type": "application/json"}
    payload = {
        "method": method,
        "params": params or {},
    }
    try:
        url = f"http://{server['ip']}:{server['port']}/rpc"
        payload = {"method": method, "params": params}
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()  # Raise HTTP errors if they occur
        return response.json()
    except requests.exceptions.Timeout:
        print(f"RPC call to {url} timed out after {timeout} seconds.")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed RPC call to {url}: {e}")