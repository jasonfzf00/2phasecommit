import json
# import os
from datetime import datetime

LOG_FILE = "server_log.json"

def init_log():
    # if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as log_file:
        json.dump([], log_file)

def write_log(entry):
    with open(LOG_FILE, "r") as log_file:
        logs = json.load(log_file)

    logs.append(entry)

    with open(LOG_FILE, "w") as log_file:
        json.dump(logs, log_file, indent=4)

def read_logs():
    with open(LOG_FILE, "r") as log_file:
        return json.load(log_file)

def log_event(state, account_id, account_balance, error):
    """
    Write a new log given state (prepare, commit), account_id, account_balance
    """
    entry = {
        "state": state,
        "timestamp": datetime.now().isoformat(),
        "account_id": account_id,
        "account_balance": account_balance,
        "error": error
    }
    write_log(entry)