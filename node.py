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
import os
import time
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
import task_originator
import monitor
import asyncio
import tensorflow as tf
from messagebus import MessageBus
import agent
import yaml
import json
from robustness_privacy_scoring import get_network_trustworthiness_score, get_device_user_type_score

TAG = "[NODE]"
CONFIG_FILE= "node_config.yaml"
NODE_STATE_FILE = "node_state.json"
HORIZON_H = 5
NODE_FAILURE_LOGGING_PERIOD = 7  # days

# Evaluation tests --------------------------------------------
TEST_SETUP_FILE = "./evaluation_tests/test_round_setups_b.json"
# -------------------------------------------------------------


class Node:
    def __init__(self, node_id: str, lan: str, nats_url: str, lstm_model_path: str, llm_model_path: str,
                 device_user_category: str, network_type: str, init_state: dict):
        self.id = node_id
        self.lan = lan
        self.peers = []  # tuple: ("lan": str, "node_id": node_id, "ip": str|None)
        self.assigned_task_counts = init_state.get("assigned-task-counts", {})  # number of tasks assigned to each peer node

        print(f"{TAG} Loading LSTM ...")
        self.lstm_model = tf.keras.models.load_model(lstm_model_path)
        self.window_len = self.lstm_model.input_shape[1]
        print(f"{TAG} LSTM ready  window={self.window_len}")

        self.resource_history = deque(maxlen=self.window_len)

        network_trust_score = get_network_trustworthiness_score(network_type)
        device_user_type_score = get_device_user_type_score(device_user_category)

        # Record the number of node failures based on the restart timestamps
        # The very first restart timestamp is not counted as a failure, since it's the initial start of the node.
        # NOTE: THIS SETUP FOR EVALUATION TESTS!
        #node_failures = len(init_state.get("restart-times", []))-1
        node_failures = init_state.get("restart_times")

        self.state = dict(score=0.5, risk="MEDIUM",
                   reputation=0.5, reliability=0.6, 
                   cpu=0.0, mem=0.0, disk=0.0,
                   cpu_pred=0.5, mem_pred=0.5, disk_pred=0.5, 
                   lstm_ready=False,
                   horizon=[[0.5,0.5,0.5]]*HORIZON_H, 
                   is_busy=False, 
                   tasks_completed=init_state.get("tasks-completed", 0), 
                   tasks_assigned=init_state.get("tasks-assigned", 0),
                   network_trust_score=network_trust_score, 
                   usr_type_score=device_user_type_score,
                   node_failures=node_failures,
                   reliability_lvl=init_state.get("reliability-lvl"),   # NOTE: for eval tests!
                   privacy_lvl=init_state.get("privacy-lvl")            # NOTE: for eval tests!
                   )
        
        #self.task_cache = init_state.get("task_cache", [])
        self.task_queue = deque()           # TODO: should this be in state json file?
        self.completed_tasks_results = init_state.get("completed-tasks-results", [])

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


    def save_node_state(self, restart_times: list):
        """Save the node's state to a JSON file."""

        state_to_save = {
            "restart-times": restart_times,
            "tasks-assigned": self.state.get("tasks_assigned", 0),
            "tasks-completed": self.state.get("tasks_completed", 0),
            "assigned-task-counts": self.assigned_task_counts,
            "completed-tasks-results": self.completed_tasks_results
        }

        with open(NODE_STATE_FILE, "w") as file:
            json.dump(state_to_save, file)

        print(f"{TAG} Stored node state to {NODE_STATE_FILE}")



if __name__ == "__main__":
    config = {}
    init_node_state = {}

# NOTE: FOR EVALUATION TESTS ----------------------------------------------
    with open(TEST_SETUP_FILE, "r") as test_setup_f:
        test_setup = json.load(test_setup_f)

    rnd = test_setup["round-1"] # NOTE: change to "round-2" for the test round 2 setup

    init_node_state = {
        "restart-times": rnd["node-failures"],
        "tasks-assigned": test_setup["tasks-assigned"],
        "tasks-completed": rnd["tasks-completed"],
        "assigned-task-counts": test_setup["assigned-task-counts"],
        "completed-tasks-results": [],
        "reliability-lvl": rnd["reliability-lvl"],
        "privacy-lvl": rnd["privacy-lvl"]
    }

# -------------------------------------------------------------------------

    with open(CONFIG_FILE) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

# NOTE: COMMENTED OUT FOR EVALUATION TESTS
#    if os.path.exists(NODE_STATE_FILE):
#        with open(NODE_STATE_FILE, "r") as file:
#            init_node_state = json.load(file)
#    else:
#        init_node_state.update({
#            "restart-times": [],
#            "tasks-assigned": 0,
#            "tasks-completed": 0,
#            "assigned-task-counts": {},
#            "completed-tasks-results": []
#        })

    print(f"{TAG} Initial node state:\n\t{init_node_state}")

# NOTE: COMMENTED OUT FOR EVALUATION TESTS
#    # Record the time of new node restart and add to the restart list
#    new_restart_time = time.time()
#    init_node_state["restart-times"].append(new_restart_time)
#
#    # Remove restart timestamps older than 7 days
#    print(f"{TAG} Remove restart timestamps older than {NODE_FAILURE_LOGGING_PERIOD} days...")
#
#    restart_times = init_node_state.get("restart-times", [])
#    if restart_times:
#        updated_restart_times = [old_restart_time for old_restart_time in restart_times 
#                                 if (new_restart_time - old_restart_time) <= NODE_FAILURE_LOGGING_PERIOD * 24 * 60 * 60]
#        init_node_state["restart-times"] = updated_restart_times

    node = Node(node_id = config["nid"], 
                lan = config["lan"], 
                nats_url = config["nats-url"],
                lstm_model_path = config["lstm-model"],
                llm_model_path = config["llm-model"],
                # NOTE: FOR EVALUATION TESTS
                device_user_category = rnd["device-user-category"],
                network_type = rnd["network-type"],
                # NOTE: COMMENTED OUT FOR EVALUATION TESTS
                #device_user_category = config.get("device-user-category", "public"),
                #network_type = config.get("network-type", "public-network"),
                init_state=init_node_state)

    # Write the updated node state back to the JSON file (restart times list is updated)
    with open(NODE_STATE_FILE, "w") as file:
        json.dump(init_node_state, file)

    try:
        agent.register(node)       # Register message handlers for NATS communication
        asyncio.run(node.start())   # Run the node
    except KeyboardInterrupt:
        print(f"\n{TAG} Node {config["nid"]} shutting down.")

    # NOTE: COMMENTED OUT FOR EVALUATION TESTS
    #finally:
    #    node.save_node_state(restart_times)       #Save the node's state to a JSON file