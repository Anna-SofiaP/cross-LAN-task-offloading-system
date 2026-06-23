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

TAG = "[NODE]"
CONFIG_FILE= "node_config.yaml"
HORIZON_H = 5


@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


class Node:
    def __init__(self, node_id: str, lan: str, nats_url: str, lstm_model_path: str, llm_model_path: str):
        self.id = node_id
        self.lan = lan
        self.peers = []  # tuple: ("lan": str, "node_id": node_id, "ip": str|None)
        self.assigned_task_counts = {}

        print(f"{TAG} Loading LSTM ...")
        self.lstm_model = tf.keras.models.load_model(lstm_model_path)
        self.window_len = self.lstm_model.input_shape[1]
        print(f"{TAG} LSTM ready  window={self.window_len}")

        self.resource_history = deque(maxlen=self.window_len)
        self.state = dict(score=0.5, risk="MEDIUM",
                   reputation=0.5, reliability=0.6, cpu=0.0, mem=0.0, disk=0.0,
                   cpu_pred=0.5, mem_pred=0.5, disk_pred=0.5, lstm_ready=False,
                   horizon=[[0.5,0.5,0.5]]*HORIZON_H, is_busy=False,
                   tasks_completed=0)
        
        self.task_cache = []
        self.task_queue = deque()

        self.task_threads_and_results = [dict] # --> [{"task_id": tid, "thread": thread, "result": result}, {...}]

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
            task_originator.start(self),
            # ZMQ loop
            #self.bus._zmq_listen_loop()
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

    node = Node(node_id = config["nid"], 
                lan = config["lan"], 
                nats_url = config["nats-url"],
                lstm_model_path = config["lstm-model"],
                llm_model_path = config["llm-model"])

    try:
        #agent.register(node)    # Register message handlers for NATS communication
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print(f"\n{TAG} Node {config["nid"]} shutting down.")