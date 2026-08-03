"""
node.py — Main entry point

The Node class is the single object that owns all state.
Application logic lives in separate module files:
  - agent.py            incoming message handlers
  - monitor.py          heartbeat and peer management
  - task_originator.py  outbound messaging

Run this file directly to start a node:
    python node.py
"""

from collections import deque
from dataclasses import dataclass
#from datetime import time
import time
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
import task_originator
import monitor
import task_assign
import asyncio
import tensorflow as tf
#import argparse
from messagebus import MessageBus
import agent
import yaml
from robustness_privacy_scoring import get_network_trustworthiness_score, get_device_user_type_score

TAG = "[NODE]"
CONFIG_FILE= "node_config.yaml"
HORIZON_H = 5
NODE_FAILURE_LOGGING_PERIOD = 7  # days


@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


class Node:
    def __init__(self, node_id: str, lan: str, nats_url: str, lstm_model_path: str, llm_model_path: str,
                 device_user_category: str, network_type: str, restarts: list = []):
        self.id = node_id
        self.lan = lan
        self.peers = []  # tuple: ("lan": str, "node_id": node_id, "ip": str|None)
        self.assigned_task_counts = {}  # NOTE: number of assigned tasks per peer?

        print(f"{TAG} Loading LSTM ...")
        self.lstm_model = tf.keras.models.load_model(lstm_model_path)
        self.window_len = self.lstm_model.input_shape[1]
        print(f"{TAG} LSTM ready  window={self.window_len}")

        self.resource_history = deque(maxlen=self.window_len)

        network_trust_score = get_network_trustworthiness_score(network_type)
        device_user_type_score = get_device_user_type_score(device_user_category)

        # Record the number of node failures based on the restart timestamps
        # The very first restart timestamp is not counted as a failure, since it's the initial start of the node.
        node_failures = len(restarts)-1 if restarts else 0

        self.state = dict(score=0.5, risk="MEDIUM",
                   reputation=0.5, reliability=0.6, 
                   cpu=0.0, mem=0.0, disk=0.0,
                   cpu_pred=0.5, mem_pred=0.5, disk_pred=0.5, 
                   lstm_ready=False,
                   horizon=[[0.5,0.5,0.5]]*HORIZON_H, 
                   is_busy=False, 
                   tasks_completed=0, tasks_assigned=0, #NOTE: high_privacy_tasks_completed=0,
                   network_trust_score=network_trust_score, usr_type_score=device_user_type_score,
                   node_failures=node_failures,)
        
        self.task_cache = []    # FIXME: put everything here: task_id, task_type_success, task_result, etc.???
        self.task_queue = deque()
        self.completed_tasks_results = []

        print(f"{TAG} Loading LLM ...")
        self.llm_tok = AutoTokenizer.from_pretrained(llm_model_path, local_files_only=True)
        self.llm_mdl = AutoModelForCausalLM.from_pretrained(
            llm_model_path, dtype=torch.float16, device_map="cpu", local_files_only=True)
        self.llm_mdl.eval()
        print(f"{TAG} LLM ready")

        # Communication layer
        self.bus = MessageBus(self, nats_url)

        # Callbacks and handlers
        self.bus.on_peer_update(self._on_peer_update)


    async def start(self):
        print(f"{TAG} Starting node {self.id}...")

        # Connect transport layer first
        await self.bus.connect()

        # Start background loops concurrently
        await asyncio.gather(
            # Monitoring loops
            monitor.heartbeat_loop(self),
            monitor.metric_loop(self),
            # Task originator loop
            #task_originator.start(self),
            # ZMQ loop
            self.bus._zmq_listen_loop()
        )


    async def _on_peer_update(self, node_id: str, info: dict):
        print(f"{TAG} Peer joined:" \
                f"   Node ID: {node_id}" \
                f"   LAN: {info['lan']}" \
                f"   IP: {info['ip']}")
        

        # If the peer is on the same LAN, add also IP address info for direct communication
        if info['lan'] == self.lan:
            print(f"{TAG} Peer {node_id} is on the same LAN\n")
            self.peers.append((info['lan'], node_id, info['ip']))
        else:
            print(f"{TAG} Peer {node_id} is on a different LAN\n")
            self.peers.append((info['lan'], node_id, None))



if __name__ == "__main__":
    config = {}

    with open(CONFIG_FILE) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    # Record the time of new node restart
    #new_restart_time = time.strftime(time.gmtime(), "%Y-%m-%dT%H:%M:%SZ")
    new_restart_time = time.time()

    # Remove restart timestamps older than 7 days
    print(f"{TAG} Remove restart timestamps older than {NODE_FAILURE_LOGGING_PERIOD} days...")

    restart_times = config.get("restart-times", [])
    if restart_times:
        for old_restart_time in restart_times:
            if new_restart_time - old_restart_time > NODE_FAILURE_LOGGING_PERIOD * 24 * 60 * 60:
                config["restart-times"].pop(0)
    else:
        config["restart-times"] = []
    
    # Add the new restart timestamp to the list
    config["restart-times"].append(new_restart_time)

    node = Node(node_id = config["nid"], 
                lan = config["lan"], 
                nats_url = config["nats-url"],
                lstm_model_path = config["lstm-model"],
                llm_model_path = config["llm-model"],
                device_user_category = config.get("device-user-category", "public"),
                network_type = config.get("network-type", "public-network"),
                restarts=config.get("restart-times", []))

    # Write the updated configuration back to the YAML file
    with open(CONFIG_FILE, 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False, indent=4)

    try:
        agent.register(node)        # Register message handlers for NATS communication
        asyncio.run(node.start())   # Run the node
    except KeyboardInterrupt:
        print(f"\n{TAG} Node {config["nid"]} shutting down.")