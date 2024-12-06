"""
Author: Jason (Zefeng) Fu
Date: 12/03/2024

The class creates a Flask server that allows RPC calls.
Provides an implementation of the 2-phase commit protocol.
"""

from flask import Flask, request, jsonify
import argparse
import os
from log_handler import *
from account_handler import *
from rpc_call import rpc_call

class Server:
    def __init__(self, host, port, server_id: str, account_id=None, account_balance=0.0):
        """
        Initialize the server with configuration.
        """
        self.host = host
        self.port = port
        self.server_id = server_id
        self.is_coordinator = True if server_id == "coordinator" else False
        self.account_id = str(account_id)
        self.app = Flask(__name__)
        self.methods = {}

        with open("cluster.json", "r") as file:
            cluster_data = json.load(file)
        self.cluster = [
            entry for entry in cluster_data if entry["server_id"] != self.server_id]

        # Initialize logs and account files
        init_log()
        if not self.is_coordinator:
            init_account_file(self.account_id, account_balance)

        self.register_routes()
        self.register_methods()

    def rpc_method(self, func):
        """
        Decorator to register an RPC method.
        """
        self.methods[func.__name__] = func
        return func

    def register_routes(self):
        """
        Register Flask routes for the RPC server.
        """
        @self.app.route('/rpc', methods=['POST'])
        def json_rpc():
            try:
                data = request.get_json()
                method = data.get('method')
                params = data.get('params', {})

                if not isinstance(params, dict):
                    return jsonify({"result": None, "error": "Params must be a dictionary"}), 400

                if method in self.methods:
                    result = self.methods[method](**params)
                    return jsonify({"result": result, "error": None})
                else:
                    return jsonify({"result": None, "error": f"Method '{method}' not found"}), 404
            except Exception as e:
                return jsonify({"result": None, "error": str(e)}), 500

    def register_methods(self):
        @self.rpc_method
        def get_logs():
            return read_logs()

        if self.is_coordinator:
            @self.rpc_method
            def transfer(account_id_from, account_id_to, amount):
                """
                Transfer from one account to another with given amount.
                """
                try:
                    # Prepare phase
                    prepare_results = self.propose_prepare(
                        account_id_from, account_id_to, amount)
                    if not all(result['canPrepare'] for result in prepare_results):
                        # Abort if any participant cannot prepare
                        self.propose_abort(
                            account_id_from, account_id_to, amount)
                        return {"status": "abort", "details": "One or more participants failed to prepare."}

                    # Commit phase
                    commit_results = self.propose_commit(
                        account_id_from, account_id_to, amount)
                    return {"status": "commit", "details": commit_results}
                except Exception as e:
                    self.propose_abort(account_id_from, account_id_to, amount)
                    return {"status": "abort", "details": str(e)}

            @self.rpc_method
            def add_bonus(bonus_percentage):
                """
                Add 1+bonus_percentage amount of bonus to account A and add the same to account B
                """
                server_A, server_B = None
                for server in self.cluster:
                    if server["account_id"] == "A":
                        server_A = server
                    else:
                        server_B = server
                try:
                    balance_A = self.rpc_call(server_A, "get_balance", params={})
                    balance_B = self.rpc_call(server_B["ip"], "get_balance", params={})
                    bonus_amount = bonus_percentage * balance_A
                    new_balance_A = balance_A + bonus_amount
                    new_balance_B = balance_B + bonus_amount
                    
                    # Prepare phase
                    prepare_res_A = self.propose_prepare(server_A, balance_A, new_balance_A)
                    prepare_res_B = self.propose_prepare(server_B, balance_B, new_balance_B)
                    
                    if not (prepare_res_A["result"]["canPrepare"] and prepare_res_B["result"]["canPrepare"]):
                        self.propose_abort(server_A, balance_A)
                        self.propose_abort(server_B, balance_B)
                    
                    # Commit phase
                    self.propose_commit(server_A, new_balance_A)
                    self.propose_commit(server_B, new_balance_B)
                    return f"Added bonus {bonus_amount} to both accounts. " \
                        f"New Balance: A: {new_balance_A}, B: {new_balance_B}"

                    
                except Exception as e:
                    return f"Cannot add bonus: {e}"

            @self.rpc_method
            def propose_prepare(server, old_balance, new_balance):
                """
                Send the prepare request to a specific participant.
                """
                log_event("prepare", server["account_id"], old_balance, None)
                try:
                    response = self.rpc_call(
                        server,
                        "handle_prepare",
                        params={"new_balance": new_balance},
                    )
                    return response["result"]
                except Exception as e:
                    return {"result": {"canPrepare": False}, "error": str(e)}

            @self.rpc_method
            def propose_commit(server, new_balance):
                """
                Send the commit request to a specific participant.
                """
                log_event("commit", server["account_id"], new_balance, None)
                try:
                    response = self.rpc_call(
                        server,
                        "handle_commit",
                        params={"new_balance": new_balance},
                    )
                    return response["result"]
                except Exception as e:
                    print(f"Failed to commit on server {server["account_id"]}")
                    return {"canCommit": False, "error": str(e)}

            @self.rpc_method
            def propose_abort(server, balance):
                """
                Send an abort request to all participants in case of failure.
                """
                log_event("abort", server["account_id"], balance, None)
                try:
                    # Simulate an RPC call to an abort method on the participant
                    self.rpc_call(
                        server["ip"],
                        server["port"],
                        "handle_abort",
                        params={},
                    )
                except Exception as e:
                    print(f"Failed to send abort to {server['server_id']}: {e}")

        else:
            @self.rpc_method
            def get_balance():
                """
                Get the current account balance.
                """
                return read_account_balance(self.account_id)

            @self.rpc_method
            def set_balance(new_balance):
                """
                Update the account balance and log the operation.
                """
                log_event("prepare", self.account_id, new_balance, None)
                update_account_balance(self.account_id, new_balance)
                return f"Account balance updated to {new_balance}."

            @self.rpc_method
            def handle_prepare(new_balance):
                """
                Handle the prepare phase of the 2-Phase Commit Protocol.
                """
                cur_balance = get_balance()
                if new_balance < 0:
                    log_event("abort", self.account_id, cur_balance, "Insufficient funds for the requested prepare request.")
                    return {
                        "result": {"canPrepare": False, "account_balance": cur_balance},
                        "error": "Insufficient funds for the requested prepare request."
                    }
                else:
                    log_event("prepare", self.account_id, new_balance, None)
                    return {
                        "result": {"canPrepare": True, "account_balance": cur_balance},
                        "error": None
                    }

            @self.rpc_method
            def handle_commit(new_balance):
                """
                Handle the commit phase of the 2-Phase Commit Protocol.
                """
                cur_balance = get_balance()
                if new_balance < 0:
                    # Log and return abort if balance is insufficient
                    log_event("abort", self.account_id, cur_balance)
                    return {
                        "result": {"canCommit": False, "account_balance": cur_balance},
                        "error": "Commit failed due to insufficient funds."
                    }
                else:
                    # Log and update balance on commit
                    log_event("commit", self.account_id, new_balance)
                    update_account_balance(self.account_id, new_balance)
                    return {
                        "result": {"canCommit": True, "account_balance": new_balance},
                        "error": None
                    }
            
            @self.rpc_method
            def handle_abort():
                cur_balance = get_balance()
                log_event("abort", self.account_id, cur_balance, None)

    def run(self):
        """
        Start the Flask server.
        """
        self.app.run(host=self.host, port=self.port, debug=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a Flask server for a coordinator or participant.")
    parser.add_argument("host", help="Host for the server (e.g., 127.0.0.1)")
    parser.add_argument(
        "port", type=int, help="Port for the server (e.g., 8000)")
    parser.add_argument("server_id", type=str,
                        help="Set to 'coordinator' if coordinator, otherwise please use account_id for server_id.")
    parser.add_argument("account_id", nargs="?",
                        help="Account ID for participants (ignored for coordinator)")
    parser.add_argument("account_balance", type=float, nargs="?",
                        help="Initial balance for participants (ignored for coordinator)")

    args = parser.parse_args()

    # Create and run the server
    server = Server(
        host=args.host,
        port=args.port,
        server_id=args.server_id,
        account_id=args.account_id,
        account_balance=args.account_balance or 0.0
    )
    server.run()
