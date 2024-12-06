import requests
import json

def rpc_call(ip, port, method, params=None):
    headers = {"Content-Type": "application/json"}
    payload = {
        "method": method,
        "params": params or {},
    }

    url = f"http://{ip}:{port}/rpc"
    payload = {"method": method, "params": params}
    response = requests.post(url, json=payload)
    response.raise_for_status()  # Raise HTTP errors if they occur
    return response.json()


# if __name__ == "__main__":
#     client = RPCCall("http://localhost:8000/rpc")

#     # Test calling the 'add' method
#     response = client.call("get_logs", {})
#     print(response['result'][0]['ip'])
