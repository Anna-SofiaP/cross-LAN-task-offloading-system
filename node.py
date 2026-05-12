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

import asyncio
#import argparse
from messagebus import MessageBus
#import agent
import monitor
#import task_originator
import yaml

TAG = "[Node]"
CONFIG_FILE= "node_config.yaml"


class Node:
    def __init__(self, node_id: str, lan: str, nats_url: str):
        # Identity and state
        self.id = node_id
        self.lan = lan

        # Communication layer
        self.bus = MessageBus(node_id=self.id, nats_url=nats_url)

        # Callbacks and handlers
        self.bus.on_peer_update(self._on_peer_update)


    async def start(self):
        print(f"{TAG} Starting node {self.id}...")

        # Connect transport layer first
        await self.bus.connect()

        # Register incoming message handlers
#        agent.register(self)

        # Start background loops concurrently
        await asyncio.gather(
            monitor.start(self),
#            task_originator.start(self),
        )

    def _on_peer_update(self, node_id: str, info: dict):
        transport = "ZeroMQ (direct)" if info.get("local") else "NATS (via broker)"
        print(f"{TAG} Peer joined: \
                Node ID: {node_id} \
                LAN: {info['lan']} \
                IP: {info['ip']} \
                via: {transport}")



if __name__ == "__main__":
#    parser = argparse.ArgumentParser(description="Start a cluster node")
#    parser.add_argument("--id", required=True, help="Unique node ID, e.g. node-a1")
#    parser.add_argument("--nats-local", required=True, help="NATS local server URL, e.g. nats://192.168.1.10:4222")
#    args = parser.parse_args()

    config = {}

    with open(CONFIG_FILE) as stream:
        try:
            config = yaml.safe_load(stream)
            print(f"{TAG} Node configuration: \n{config}")
        except yaml.YAMLError as exc:
            print(exc)

#    node = Node(node_id=args.id, nats_url=args.nats)
    node = Node(node_id = config["nid"], 
                lan = config["lan"], 
                nats_url = config["nats-url"])

    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print(f"\n{TAG} Node {config["nid"]} shutting down.")