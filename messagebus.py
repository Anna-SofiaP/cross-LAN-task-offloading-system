"""
messagebus.py — Communication abstraction layer

Handles all transport logic:
  - ZeroMQ for direct D2D communication within the same LAN
  - NATS for cross-LAN communication via the broker/leaf node

All communication looks identical to the application layer.
"""

import asyncio
import json
import socket
from time import time
from xml.sax import handler
#import zmq
import zmq.asyncio
import nats
from dataclasses import dataclass, asdict
from typing import Callable, Optional

TAG = "[BUS]"
TOPIC_HEARTBEAT = "heartbeat"
TOPIC_TASK_REQUEST = "task_request"
BID_TIMEOUT = 90.0


@dataclass
class Message:
    """Generic message format for all communication.

     - type: determines how the message is handled by the receiving node, can be for example 'task_req', 'bid'
     - originator_node: the node_id of the sender, used for routing replies.
     - originator_lan: the LAN name/id of the sender.
     - payload: can contain any data relevant to the message type."""
    
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


def get_local_ip() -> str:
    """Get this machine's LAN IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        return ip
    except: 
        return "127.0.0.1"
    finally:
        s.close()



class MessageBus:
    ZMQ_PORT = 5555

    def __init__(self, node, nats_url, heartbeat_interval=30):
        self.node = node                # The node object is passed to the messagebus
        self.nats_url = nats_url
        self.peers = []
        self.local_ip = get_local_ip()

        self.heartbeat_interval = heartbeat_interval

        self._handlers: dict[str, Callable] = {}
        self._peer_callbacks: list[Callable] = []

        # NATS
        self.nc = None

        # ZeroMQ
        self.ctx = zmq.asyncio.Context()
        self.req_sock = None
        self.rep_sock = None

    
    # ==================================================================
    # Public API — used by Agent and TaskOriginator
    # =================================================================

    def on(self, msg_type: str, handler: Callable):
        """Register a handler for an incoming message type."""
        self._handlers[msg_type] = handler


    def on_peer_update(self, callback: Callable):
        """Register a callback that fires whenever the peer list changes."""
        self._peer_callbacks.append(callback)


    async def request(self, to: tuple, msg: Message, timeout: float = 30.0) -> Message:
        """Send a message and wait for a reply. Transport chosen automatically."""

        lan, topic, ip = to

        print(f"{TAG} Sending request to node {topic} in LAN {lan}")

        if self.node.lan == lan:    # If the target node is on the same LAN, use ZeroMQ for direct communication
            if not ip:
                return
            print(f"{TAG} Using ZeroMQ for local request")
            return await self._send_zmq(ip, msg)
        else:
            print(f"{TAG} Using NATS for remote request")
            return await self._request_nats(topic, msg, timeout)


    async def publish_heartbeat(self, lan: str):
        """Broadcast a heartbeat to the whole cluster via NATS."""
        #print(f"{TAG} Publishing heartbeat signal...\n")
        if self.nc is None:
            return
        await self.nc.publish(TOPIC_HEARTBEAT, json.dumps({
            "node_id": self.node.id,
            "lan": lan,
            "ip": self.local_ip,
        }).encode())


    # ==================================================================
    # CONNECTTION MANAGEMENT
    # ===============================================================

    async def connect(self):
        """Connect to NATS server and bind to ZMQ port."""
        await self._connect_nats()

        print(f"{TAG} Setting up ZeroMQ REP socket for incoming requests...")
        self.rep_sock = self.ctx.socket(zmq.REP)
        self.rep_sock.bind(f"tcp://0.0.0.0:{self.ZMQ_PORT}")

        #asyncio.create_task(self._zmq_listen_loop())

        print(f"{TAG} Node {self.node.id} connected. Local IP: {self.local_ip}")


    async def _connect_nats(self):
        """Connect to NATS server and subscribe to necessary subjects."""
        print(f"{TAG} Connecting to NATS at {self.nats_url}...")

        self.nc = await nats.connect(
            self.nats_url,
            reconnected_cb=self._on_nats_reconnect,
            disconnected_cb=self._on_nats_disconnect,
            error_cb=self._on_nats_error,
        )

        # Subscribe to this node's direct subject
        await self.nc.subscribe(f"nodes.{self.node.id}", cb=self._on_nats_message)
        # Subscribe to cluster-wide heartbeats
        await self.nc.subscribe(TOPIC_HEARTBEAT, cb=self._on_heartbeat)

        print(f"\n{TAG} Subscribed to nodes.{self.node.id} and {TOPIC_HEARTBEAT}\n")


    # ==================================================================
    # PEER MANAGEMENT
    # =================================================================

    def _is_local(self, lan: str) -> bool:
        """Check if the node in question is on the same LAN (i.e. we have a direct ZMQ connection)"""
        return lan is not None and self.node.lan == lan


    async def _update_peer(self, node_id: str, lan: str, ip: str):
        """Add a new peer to the registry or update an existing one. 
        If it is a new peer, call the registered callbacks.
        
        Currently there is only one callback registered by the Node class to just print the new peer info.
        """

        existed = False

        for peer in self.peers:
            if peer["node_id"] == node_id:
                existed = True
                break
        
        last_seen = asyncio.get_event_loop().time()

        if not existed:
            self.peers.append({"node_id": node_id, "last_seen": last_seen})

            print(f"{TAG} New peer discovered: {node_id} @ {ip}\n")

            for cb in self._peer_callbacks:
                await cb(node_id, {"ip": ip, "lan": lan})

        else:
            for peer in self.peers:
                if peer["node_id"] == node_id:
                    peer["last_seen"] = last_seen
                    print(f"{TAG} Peer {node_id} last seen: {peer["last_seen"]}")
                    break


    '''def get_available_peers(self) -> list[str]:
        """Return node IDs of all peers currently connected to the cluster."""
        return [nid for nid, info in self.peers.items()]'''


    # ==================================================================
    # NATS transport
    # ==================================================================

    async def _request_nats(self, topic: str, msg: Message, timeout: float) -> Message:
        try:
            print(f"{TAG} Sending NATS request to {topic} with payload: {msg.payload}")
            reply = await self.nc.request(
                f"nodes.{topic}",
                json.dumps(asdict(msg)).encode(),
                timeout=timeout,
            )

            reply_data = reply.data.decode()
            reply_JSON = json.loads(reply_data)

            print(f"\n{TAG} Received NATS reply from {topic}: {reply_data}\n")

            response = Message(**reply_JSON)
            return response
        except Exception as e:
            print(f"{TAG} NATS request to {topic} failed: {e}")
            raise


    async def _on_nats_message(self, raw_msg):
        """Dispatch an incoming NATS direct message to the registered handler."""
        try:
            msg = Message(**json.loads(raw_msg.data.decode()))
            print(f"{TAG} Received NATS message of type {msg.type} from {msg.originator_node}\n")
            response = await self._dispatch(msg)
            if raw_msg.reply and response is not None:
                await self.nc.publish(raw_msg.reply, json.dumps(asdict(response)).encode())
        except Exception as e:
            print(f"{TAG} Error handling NATS message: {e}")
            

    async def _on_heartbeat(self, raw_msg):
        """Handle an incoming heartbeat from any node in the cluster."""
        try:
            data = json.loads(raw_msg.data)
            node_id = data["node_id"]

            print(f"{TAG} Heartbeat signal from node: {node_id}")

            if node_id == self.node.id:
                return  # ignore own heartbeat
            
            await self._update_peer(node_id, data["lan"], data["ip"])
        except Exception as e:
            print(f"{TAG} Error handling heartbeat: {e}")


    async def _on_nats_reconnect(self):
        print(f"{TAG} Reconnected to NATS")
        print(f"{'-'*20}\n")

    async def _on_nats_disconnect(self):
        print(f"{TAG} Disconnected from NATS")
        print(f"{'-'*20}\n")

    async def _on_nats_error(self, e):
        print(f"{TAG} NATS error: {e}")
        print(f"{'-'*20}\n")


    # ==================================================================
    # ZeroMQ transport
    # ==================================================================

    async def _send_zmq(self, ip: str, msg: Message) -> Message:
        """Send a message through the ZMQ request socket."""
        print(f"{TAG} Sending ZMQ message to {ip}...")
        try:
            req_sock = self.ctx.socket(zmq.REQ)
            #req_sock.setsockopt(zmq.RCVTIMEO, 5000)
            req_sock.setsockopt(zmq.LINGER, 0)
            req_sock.connect(f"tcp://{ip}:{self.ZMQ_PORT}")

            req = json.dumps({
                "type": msg.type,
                "originator_node": msg.originator_node,
                "originator_lan": msg.originator_lan,
                "payload": msg.payload
            })

            await req_sock.send_string(req)
            ack = await req_sock.recv_string()  # waits for REP to reply
            response = json.loads(ack)
            response = Message(**response)

            return response
        except Exception as e:
            print(f"{TAG} Sending message via ZMQ to {ip} failed: {e}")
            raise
        finally:
            req_sock.close()


    async def _zmq_listen_loop(self):
        """Runs as a background task — receives, dispatches, replies."""
        print(f"{TAG} Starting ZeroMQ loop for receiving messages from peers...\n")
        while True:
            try:
                raw = await self.rep_sock.recv_string()
                data = json.loads(raw)
                msg = Message(**data)

                # Dispatch to registered handler and get reply
                reply = await self._dispatch(msg)

                await self.rep_sock.send_string(json.dumps(asdict(reply)))

            except Exception as e:
                print(f"{TAG} ZMQ listen loop error: {e}")
                await self.rep_sock.send_string(json.dumps({"msg": "error"}))
                # always send something back or the REQ side hangs

    
    async def _dispatch(self, msg: Message) -> Message:
        print(f"{TAG} Dispatching message of type {msg.type} to handler...\n")

        handler = self._handlers.get(msg.type)

        if handler:
            result = handler(self.node, msg.payload, msg.originator_lan, msg.originator_node)

            if asyncio.iscoroutine(result):
                result = await result

            response = Message(type=result["type"], 
                               originator_node=self.node.id, 
                               originator_lan=self.node.lan,
                               payload=result["payload"])
            return response
        
        else:
            print(f"{TAG} No handler for message type: {msg.type}\n")
            return None