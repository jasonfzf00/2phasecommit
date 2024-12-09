"""
Author: Jason (Zefeng) Fu
Date: 12/03/2024

Server interface for the 2-phase commit protocol.
"""

from flask import Flask, request, jsonify
from flask_apscheduler import APScheduler
import argparse
import os
import time
from log_handler import *
from account_handler import *
from rpc_call import rpc_call

class Config:
    SCHEDULER_API_ENABLED = True

class BaseServer:
    def __init__(self, host, port, server_id: str):
        """Initialize the base server with common configuration."""
        self.host = host
        self.port = port
        self.server_id = server_id
        self.app = Flask(__name__)
        
        with open("cluster.json", "r") as file:
            cluster_data = json.load(file)
        self.cluster = [
            entry for entry in cluster_data if entry["server_id"] != self.server_id]

        self.register_routes()
        self.register_methods()
        
        # Timeout detection
        self.transaction_counter = 0
        self.transactions = {}
        self.timeout = 3
        
    def register_routes(self):
        """Register Flask routes for the RPC server."""
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
        """Register common RPC methods."""
        self.methods = {}
        self.methods["get_logs"] = self.get_logs

    def get_logs(self):
        """Return the logs for debugging."""
        return read_logs()

    def run(self):
        """Start the Flask server."""
        self.app.run(host=self.host, port=self.port, debug=True)