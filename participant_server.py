from base_server import BaseServer
from account_handler import *
from log_handler import *
from flask_apscheduler import APScheduler
from rpc_call import rpc_call
import time
import argparse

class ParticipantServer(BaseServer):
    def __init__(self, host, port, account_id, account_balance=0.0):
        """Initialize the participant server."""
        super().__init__(host, port, account_id)
        self.account_id = str(account_id)
        
        # Initialize account
        init_log()
        init_account_file(self.account_id, account_balance)
        
        # Initialize scheduler for timeout detection
        self.scheduler = APScheduler()
        self.scheduler.init_app(self.app)
        self.scheduler.start()
        self.scheduler.add_job(
            id="timeout_monitor",
            func=self.monitor_timeout,
            trigger="interval",
            seconds=3,
        )

    def register_methods(self):
        """Register participant-specific RPC methods."""
        super().register_methods()
        self.methods.update({
            "get_balance": self.get_balance,
            "set_balance": self.set_balance,
            "handle_prepare": self.handle_prepare,
            "handle_commit": self.handle_commit,
            "handle_abort": self.handle_abort,
            "recover": self.recover
        })

    def monitor_timeout(self):
        """Monitor transactions for timeout and detect coordinator failure."""
        current_time = time.time()
        for transaction_id, transaction in list(self.transactions.items()):
            time_elapsed = current_time - transaction["timestamp"]
            if time_elapsed > self.timeout:
                print(f"Timeout detected for transaction {transaction_id}. Coordinator failed.")
                self.handle_abort(transaction_id)
                
    def get_balance(self):
        """
        Get the current account balance.
        """
        return read_account_balance(self.account_id)

    def set_balance(self, new_balance):
        """
        Update the account balance and log the operation.
        """
        log_event("commit", self.account_id, new_balance, None)
        update_account_balance(self.account_id, new_balance)
        return f"Account balance updated to {new_balance}"

    def handle_prepare(self, new_balance, transaction_id):
        """
        Handle the prepare phase of the 2-Phase Commit Protocol.
        """
        if new_balance < 0:
            return {"result": {"canPrepare": False}, "error": "Insufficient funds"}
        else:
            log_event("prepare", self.account_id, new_balance, None)
            self.transactions[transaction_id] = {
                "new_balance": new_balance,
                "timestamp": time.time(),
            }
            return {"result": {"canPrepare": True}, "error" : None}

    def handle_commit(self, transaction_id):
        """
        Handle the commit phase of the 2-Phase Commit Protocol.
        """
        if transaction_id in self.transactions:
            new_balance = self.transactions[transaction_id]["new_balance"]
            log_event("commit", self.account_id, new_balance, None)
            self.set_balance(new_balance)
            del self.transactions[transaction_id] 
            return {"result": {"canCommit": True},}
        else:
            return {"result": {"canCommit": False}, "error": "Transaction not prepared"}

    def handle_abort(self, transaction_id = None):
        """
        Handle the abort phase of the 2-Phase Commit Protocol.
        """
        current_balance = self.get_balance()
        log_event("abort", self.account_id, current_balance, "Transaction aborted")
        if transaction_id:
            print(f"Transaction {transaction_id} aborted.")
            if transaction_id in self.transactions:
                del self.transactions[transaction_id]
        else:
            self.transactions.clear()
        
    def recover(self):
        coordinator = None
        for server in self.cluster:
            if server["server_id"] == "coordinator":
                coordinator = server
                break
            
        if not coordinator:
            print("No coordinator found in the cluster. Recovery failed.")
            return
        
        try:
            logs = rpc_call(coordinator, "get_logs", params={})
            relevant_logs = [log for log in logs
            if log["account_id"] == self.account_id and log["state"] == "commit"]
            
            for log in relevant_logs:
                new_balance = log["account_balance"]
                print(f"Recovering: Applying committed balance {new_balance} for account {self.account_id}.")
                update_account_balance(self.account_id, new_balance)
                log_event(
                    state="recovered",
                    account_id=self.account_id,
                    account_balance=new_balance,
                    error=None
                )
            if relevant_logs:
                print(f"Recovery complete for account {self.account_id}.")
            else:
                print(f"No committed logs found for account {self.account_id}. Recovery not needed.")
                
        except Exception as e:
            print(f"Failed to recover state for account {self.account_id}: {e}")

    def mock_failure(self):
        print(f"Simulating failure: Sleeping for 10 seconds.")
        time.sleep(10)
        self.recover()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a Flask server for a coordinator or participant.")
    parser.add_argument("host", help="Host for the server (e.g., 127.0.0.1)")
    parser.add_argument(
        "port", type=int, help="Port for the server (e.g., 8000)")
    parser.add_argument("account_id", nargs="?",
                        help="Account ID for participants (ignored for coordinator)")
    parser.add_argument("account_balance", type=float, nargs="?",
                        help="Initial balance for participants (ignored for coordinator)")

    args = parser.parse_args()

    # Create and run the server
    server = ParticipantServer(
        host=args.host,
        port=args.port,
        account_id=args.account_id,
        account_balance=args.account_balance or 0.0
    )
    server.run()
