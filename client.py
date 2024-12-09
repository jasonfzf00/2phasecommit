from rpc_call import rpc_call
import json

with open("cluster.json", "r") as file:
    cluster_data = json.load(file)
    
coordinator = None
for server in cluster_data:
    if server["server_id"] == "coordinator":
        coordinator = server
        
add_bonus = rpc_call(coordinator, "add_bonus", params={"bonus_percentage": 0.2})
# transfer = rpc_call(coordinator, "transfer", params={"account_id_from": "A", "account_id_to": "B", "amount": 100.00})