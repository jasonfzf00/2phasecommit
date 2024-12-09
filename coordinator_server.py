from base_server import BaseServer
from log_handler import *
from flask_apscheduler import APScheduler
from rpc_call import rpc_call
import time
import argparse

class CoordinatorServer(BaseServer):
    def __init__(self, host, port):
        """Initialize the coordinator server."""
        super().__init__(host, port, "coordinator")
        init_log()
        
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
        """Register coordinator-specific RPC methods."""
        super().register_methods()
        self.methods.update({
            "get_logs": self.get_logs,
            "transfer": self.transfer,
            "add_bonus": self.add_bonus,
            "propose_prepare": self.propose_prepare,
            "propose_commit": self.propose_commit,
            "propose_abort": self.propose_abort
        })

    def monitor_timeout(self):
        """Monitor transactions for timeout."""
        current_time = time.time()
        for transaction_id, transaction in list(self.transactions.items()):
            if transaction["responses"]:
                continue
            time_elapsed = current_time - transaction["timestamp"]
            if time_elapsed > self.timeout:
                print(f"Timeout detected for transaction {transaction_id}.")
                self.handle_abort(transaction_id)

    def get_logs(self):
        """
        Return the logs for debugging.
        """
        return read_logs()
    
    def transfer(self, account_id_from, account_id_to, amount):
        """
        Transfer from one account to another with the given amount.
        """
        server_A, server_B = None, None
        for server in self.cluster:
            if server["account_id"] == "A":
                server_A = server
            else:
                server_B = server
        participants = [server_A, server_B]

        try:
            balance_A = rpc_call(server_A, "get_balance", params={})["result"]
            balance_B = rpc_call(server_B, "get_balance", params={})["result"]

            if account_id_from == "A":
                new_balance_A = balance_A - amount
                new_balance_B = balance_B + amount
            else:
                new_balance_A = balance_A + amount
                new_balance_B = balance_B - amount

            new_balances = {"A": new_balance_A, "B": new_balance_B}
            old_balances = {"A": balance_A, "B": balance_B}
            
            self.propose_prepare(participants, old_balances, new_balances)
            
            transaction = self.transactions[self.transaction_counter]
            responses = transaction["responses"]
            if all(response["result"]["canPrepare"] for response in responses.values()):
                return f"Transferred {amount} from {account_id_from} to {account_id_to}. " \
                    f"New Balances: A: {new_balances['A']}, B: {new_balances['B']}"
            else:
                return "Failed to transfer. Transaction aborted."
        except TimeoutError:
            return f"Failed to transfer because of timeout"
        except Exception as e:
            return f"Transfer failed: {e}"

    def add_bonus(self, bonus_percentage):
        """
        Add a bonus (percentage) to account A and account B.
        """
        server_A, server_B = None, None
        for server in self.cluster:
            if server["account_id"] == "A":
                server_A = server
            else:
                server_B = server
        participants = [server_A, server_B]
        
        try:
            # Validate bonus percentage
            if bonus_percentage < 0:
                return "Failed to add bonus due to invalid bonus. Transaction aborted."

            balance_A = rpc_call(server_A, "get_balance", params={})["result"]
            balance_B = rpc_call(server_B, "get_balance", params={})["result"]
            bonus_amount = bonus_percentage * balance_A
            new_balances = {"A": balance_A + bonus_amount, "B": balance_B + bonus_amount}
            old_balances = {"A": balance_A, "B": balance_B}
            
            # Call propose_prepare but don't rely on its return value
            self.propose_prepare(participants, old_balances, new_balances)
            
            # Check transaction status directly
            transaction = self.transactions[self.transaction_counter]
            responses = transaction["responses"]
            if all(response["result"]["canPrepare"] for response in responses.values()):
                return f"Added bonus {bonus_amount} to both accounts. " \
                    f"New Balances: A: {new_balances['A']}, B: {new_balances['B']}"
            else:
                return "Failed to add bonus. Transaction aborted."
        except TimeoutError:
            return "Failed to add bonus because of timeout"
        except Exception as e:
            return f"Failed to add bonus. Error occured: {e}. Transaction aborted."

    def propose_prepare(self, participants, old_balances, new_balances):
        """
        Send the prepare request to a specific participant.
        """
        self.transaction_counter += 1
        self.transactions[self.transaction_counter] = {
            "participants": participants,
            "responses": {},
            "old_balances": old_balances,
            "new_balances": new_balances,
            "timestamp": time.time(),
        }
        
        for participant in participants:
            log_event("prepare", participant["account_id"], new_balances[participant["account_id"]], None)
            try:
                response = rpc_call(participant, "handle_prepare", 
                                  params={"new_balance": new_balances[participant["account_id"]], 
                                        "transaction_id": self.transaction_counter})
                self.transactions[self.transaction_counter]["responses"][participant["account_id"]] = response
            except TimeoutError:
                print(f"Timeout detected for participant {participant['server_id']} during prepare.")
                self.transactions[self.transaction_counter]["responses"][participant["account_id"]] = {
                    "result": {"canPrepare": False},
                    "error": "Timeout",
                }
            except Exception as e:
                self.transactions[self.transaction_counter]["responses"][participant["account_id"]] = {
                    "result": {"canPrepare": False},
                    "error": str(e)
                }
                
        self.process_prepare_responses(self.transaction_counter)

    def process_prepare_responses(self, transaction_id):
        """
        Process responses from participants during the prepare phase.
        """
        transaction = self.transactions.get(transaction_id)
        if not transaction:
            print(f"Transaction {transaction_id} not found.")
            return

        responses = transaction["responses"]
        all_prepared = all(
            response["result"]["canPrepare"]
            for response in responses.values()
            if "result" in response
        )

        if all_prepared:
            print(f"All participants prepared for transaction {transaction_id}. Proceeding to commit.")
            self.propose_commit(transaction_id)
        else:
            print(f"Prepare phase failed for transaction {transaction_id}. Aborting.")
            self.propose_abort(transaction_id)

    def propose_commit(self, transaction_id):
        """
        Send the commit request to a specific participant.
        """
        transaction = self.transactions.get(transaction_id)
        if not transaction:
            print(f"Transaction {transaction_id} not found.")
            return False
        
        success = True
        for participant in transaction["participants"]:
            log_event("commit", participant["account_id"], transaction["new_balances"][participant["account_id"]], None)
            try:
                response = rpc_call(participant, "handle_commit", params={"transaction_id": transaction_id})
                if not response["result"].get("canCommit", False):
                    print(f"Commit failed for participant {participant['server_id']}")
                    success = False
                    break
            except Exception as e:
                print(f"Error during commit for participant {participant['server_id']}: {e}")
                success = False
                break
            
        if not success:
            return self.propose_abort(transaction_id)
            
        print(f"Transaction {transaction_id} committed successfully.")
        del self.transactions[transaction_id]
        return True

    def propose_abort(self, transaction_id):
        """
        Send an abort request to a specific participant.
        """
        transaction = self.transactions.get(transaction_id)
        if not transaction:
            print(f"Transaction {transaction_id} not found.")
            return
        
        for participant in transaction["participants"]:
            log_event("abort", participant["account_id"], transaction["old_balances"][participant["account_id"]], None)
            try:
                rpc_call(participant, "handle_abort", params={"transaction_id": transaction_id})
                print(f"Aborted transaction {transaction_id} on participant {participant['server_id']}.")
            except TimeoutError:
                print(f"Timeout detected for participant {participant['server_id']} during abort.")
            except Exception as e:
                print(f"Failed to send abort to {participant['server_id']}: {e}")
        
        print(f"Transaction {transaction_id} aborted.")
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a Flask server for a coordinator or participant.")
    parser.add_argument("host", help="Host for the server (e.g., 127.0.0.1)")
    parser.add_argument(
        "port", type=int, help="Port for the server (e.g., 8000)")

    args = parser.parse_args()

    # Create and run the server
    server = CoordinatorServer(
        host=args.host,
        port=args.port,
    )
    server.run()