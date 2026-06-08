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

import task_originator
import monitor
import asyncio
#import argparse
from messagebus import MessageBus
import agent
import yaml

TAG = "[Node]"
CONFIG_FILE= "node_config.yaml"


class Node:
    def __init__(self, node_id: str, lan: str, nats_url: str):
        # Identity and state
        self.id = node_id
        self.lan = lan
        self.peers = []  # tuple: ("lan": str, "node_id": node_id, "ip": str|None)

        # Communication layer
        self.bus = MessageBus(node_id=node_id, nats_url=nats_url, lan=lan)

        # Callbacks and handlers
        self.bus.on_peer_update(self._on_peer_update)


    async def start(self):
        print(f"{TAG} Starting node {self.id}...")

        # Connect transport layer first
        await self.bus.connect()

        # Register incoming message handlers
        #agent.register(self)

        # Start background loops concurrently
        await asyncio.gather(
            #monitor.start(self),
            monitor.heartbeat_loop(self),
            task_originator.start(self),
            self.bus._zmq_listen_loop()
            #agent.start(self)
        )


    def _on_peer_update(self, node_id: str, info: dict):
        transport = "ZeroMQ (direct)" if info.get("local") else "NATS (via broker)"
        print(f"{TAG} Peer joined:" \
                f"   Node ID: {node_id}" \
                f"   LAN: {info['lan']}" \
                f"   IP: {info['ip']}" \
                f"   via: {transport}")
        

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

#    node = Node(node_id=args.id, nats_url=args.nats)
    node = Node(node_id = config["nid"], 
                lan = config["lan"], 
                nats_url = config["nats-url"])

    try:
        #agent.register(node)    # Register message handlers for NATS communication
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print(f"\n{TAG} Node {config["nid"]} shutting down.")