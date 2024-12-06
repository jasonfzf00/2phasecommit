import os
import json

ACCOUNT_FILE = "account.json"

def init_account_file(account_id, balance=0.0):
    # if not os.path.exists(ACCOUNT_FILE):
    account = {}
    account[account_id] = balance
    with open(ACCOUNT_FILE, "w") as account_file:
            json.dump(account, account_file,indent=4) 

def read_account_balance(account_id):
    with open(ACCOUNT_FILE, "r") as account_file:
        accounts = json.load(account_file)

    if account_id in accounts:
        return accounts[account_id]
    else:
        return None

def update_account_balance(account_id, new_balance):
    with open(ACCOUNT_FILE, "r") as account_file:
        accounts = json.load(account_file)

    accounts[account_id] = new_balance

    with open(ACCOUNT_FILE, "w") as account_file:
        json.dump(accounts, account_file, indent=4)