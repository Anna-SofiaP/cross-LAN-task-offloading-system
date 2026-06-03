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
from xml.sax import handler
import zmq
import zmq.asyncio
import nats
from dataclasses import dataclass, asdict
from typing import Callable, Optional
from agent import ack_task_request

TAG = "[MessageBus]"
TOPIC_HEARTBEAT = "heartbeat"
TOPIC_TASK_REQUEST = "task_request"


@dataclass
class Message:
    """Generic message format for all communication.
     The 'type' field determines how the message is handled by the receiving node.

     payload: can contain any data relevant to the message type.
     type: can be for example 'task_req_ack', 'bid'
     originator_node: the node_id of the sender, used for routing replies.
     originator_lan: the LAN of the sender, used for routing and debugging."""
    
    type: str
    originator_node: str
    originator_lan: str
    payload: dict


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


def same_subnet(ip1: str, ip2: str, prefix_len: int = 24) -> bool:
    """Check if two IPs are on the same /24 subnet."""
    def to_int(ip):
        parts = ip.split(".")
        return sum(int(p) << (8 * (3 - i)) for i, p in enumerate(parts))

    mask = ((1 << 32) - 1) ^ ((1 << (32 - prefix_len)) - 1)
    return (to_int(ip1) & mask) == (to_int(ip2) & mask)


class MessageBus:
    ZMQ_PORT = 5555

    def __init__(self, node_id: str, nats_url: str, lan: str, heartbeat_interval=30):
        self.node_id = node_id
        self.nats_url = nats_url
        self.lan = lan
        self.local_ip = get_local_ip()

        self.heartbeat_interval = heartbeat_interval

        # Peer registry: node_id → {"ip": ..., "status": ..., "last_seen": ..., "local": bool}
        self.peers: dict[str, dict] = {}

        self._handlers: dict[str, Callable] = {}
        self._peer_callbacks: list[Callable] = []

        # NATS
        self.nc = None

        # ZeroMQ
        self._zmq_ctx = zmq.asyncio.Context()
        self._zmq_router: Optional[zmq.asyncio.Socket] = None  # listens for incoming
        self._zmq_dealers: dict[str, zmq.asyncio.Socket] = {}  # node_id → dealer socket

    
    # ==================================================================
    # Public API — used by Agent and TaskOriginator
    # =================================================================

    def on(self, msg_type: str, handler: Callable):
        """Register a handler for an incoming message type."""
        self._handlers[msg_type] = handler


    def on_peer_update(self, callback: Callable):
        """Register a callback that fires whenever the peer list changes."""
        self._peer_callbacks.append(callback)


    '''
    async def send(self, to: str, msg: Message):
        """Send a fire-and-forget message to a node. Transport chosen automatically."""
        if self._is_local(to):
            await self._send_zmq(to, msg)
        else:
            await self._pub_nats(to, msg)'''

    
    async def request(self, to: tuple, msg: Message, timeout: float = 3.0) -> Message:
        """Send a message and wait for a reply. Transport chosen automatically."""
        lan, topic = to
        print(f"{TAG} Sending request to {topic} in LAN {lan} with timeout {timeout}s")
        #if self._is_local(lan):
        #    return await self._request_zmq(to, msg, timeout)
        #else:
        #    return await self._request_nats(to, msg, timeout)
        return await self._request_nats(topic, msg, timeout)


    async def publish_heartbeat(self, lan: str):
        """Broadcast a heartbeat to the whole cluster via NATS."""
        if self.nc is None:
            return
        await self.nc.publish(TOPIC_HEARTBEAT, json.dumps({
            "node_id": self.node_id,
            "lan": lan,
            "ip": self.local_ip,
        }).encode())


    # ==================================================================
    # CONNECTTION MANAGEMENT
    # ===============================================================

    async def connect(self):
        """Connect to NATS server and start ZMQ listener."""
        await self._connect_nats()
#        await self._start_zmq_listener()   # TODO: later!
        print(f"{TAG} Node {self.node_id} connected. Local IP: {self.local_ip}")


    async def _connect_nats(self):
        """Connect to NATS server and subscribe to necessary subjects."""
        self.nc = await nats.connect(
            self.nats_url,
            reconnected_cb=self._on_nats_reconnect,
            disconnected_cb=self._on_nats_disconnect,
            error_cb=self._on_nats_error,
        )
        # Subscribe to this node's direct subject
        await self.nc.subscribe(f"nodes.{self.node_id}", cb=self._on_nats_message)
        # Subscribe to cluster-wide task requests
        #await self.nc.subscribe(TOPIC_TASK_REQUEST, cb=self._on_task_request)
        # Subscribe to cluster-wide heartbeats
        await self.nc.subscribe(TOPIC_HEARTBEAT, cb=self._on_heartbeat)
        print(f"{TAG} Subscribed to nodes.{self.node_id}, {TOPIC_TASK_REQUEST}, and {TOPIC_HEARTBEAT}")

    # TODO: do some kind of connecting to a ZMQ socket here!
    '''async def _start_zmq_listener(self):
        """Start a ZMQ ROUTER socket to receive direct messages from local peers."""
        self._zmq_router = self._zmq_ctx.socket(zmq.ROUTER)
        self._zmq_router.bind(f"tcp://0.0.0.0:{self.ZMQ_PORT}")
        asyncio.create_task(self._zmq_receive_loop())
        print(f"{TAG} ZeroMQ listener on port {self.ZMQ_PORT}")'''


    # ==================================================================
    # PEER MANAGEMENT
    # =================================================================

    def _is_local(self, lan: str) -> bool:
        """Check if the node in question is on the same LAN (i.e. we have a direct ZMQ connection)"""
        return lan is not None and self.lan == lan


    def _update_peer(self, node_id: str, lan: str, ip: str):
        """Add a new peer to the registry or update an existing one. 
        If it is a new peer, call the registered callbacks.
        
        Currently there is only one callback registered by the Node class to just print the new peer info.
        """

        is_local = same_subnet(self.local_ip, ip)
        existed = node_id in self.peers
        self.peers[node_id] = {
            "ip": ip,
            "lan": lan,
            "last_seen": asyncio.get_event_loop().time(),
            "local": is_local,
        }
        if not existed:
            transport = "ZeroMQ (direct)" if is_local else "NATS (via broker)"
            print(f"{TAG} New peer discovered: {node_id} @ {ip} — transport: {transport}")
            for cb in self._peer_callbacks:
                cb(node_id, self.peers[node_id])

    # TODO: make work!
    '''def get_available_peers(self) -> list[str]:
        """Return node IDs of all peers currently conneced to the cluster."""
        return [nid for nid, info in self.peers.items()]'''


    # ==================================================================
    # NATS transport
    # _pub_nats() → publish a message to another node's subject (pub/sub)
    # _request_nats() → send a message and wait for a reply (request/reply)
    # _on_nats_message() → handle an incoming NATS message, dispatch to handler
    # _on_heartbeat() → handle an incoming heartbeat, update peer registry
    # _on_nats_reconnect() / _on_nats_disconnect() / _on_nats_error() → log connection status

# TODO: make these work!
    '''async def _pub_nats(self, to: str, msg: Message):
        await self.nc.publish(f"nodes.{to}", json.dumps(asdict(msg)).encode())'''


    async def _request_nats(self, topic: str, msg: Message, timeout: float) -> Message:
        try:
            reply = await self.nc.request(
                f"nodes.{topic}",
                json.dumps(asdict(msg)).encode(),
                timeout=timeout,
            )
            response = Message("ack", self.node_id, self.lan, payload=json.loads(reply.data.decode()))
            return response
        except Exception as e:
            print(f"{TAG} NATS request to {topic} failed: {e}")
            #return None
            raise


    async def _on_nats_message(self, raw_msg):
        """Dispatch an incoming NATS direct message to the registered handler."""
        try:
            msg = Message(**json.loads(raw_msg.data.decode()))
            print(f"{TAG} Received NATS message of type {msg.type} from {raw_msg.subject}")
            response = await self._dispatch(msg)
            if raw_msg.reply and response is not None:
                await self.nc.publish(raw_msg.reply, json.dumps(response).encode())
        except Exception as e:
            print(f"{TAG} Error handling NATS message: {e}")

    '''
    async def _on_task_request(self, raw_req):
        """Handle an incoming cluster-wide task request."""
        try:
            task_req = Message(**json.loads(raw_req.data.decode()))
            print(f"{TAG} Received task request: {task_req.type}, id={task_req.payload['task_type']} from {task_req.originator_node} in LAN {task_req.originator_lan}")
            if task_req.originator_node == self.node_id:
                return  # ignore own task 
        
            self._dispatch(task_req)
        except Exception as e:
            print(f"{TAG} Error handling task request: {e}")'''
            

    async def _on_heartbeat(self, raw_msg):
        """Handle an incoming heartbeat from any node in the cluster."""
        try:
            data = json.loads(raw_msg.data)
            node_id = data["node_id"]
            if node_id == self.node_id:
                return  # ignore own heartbeat
            self._update_peer(node_id, data["lan"], data["ip"])
        except Exception as e:
            print(f"{TAG} Error handling heartbeat: {e}")


    async def _on_nats_reconnect(self):
        print(f"{TAG} Reconnected to NATS")

    async def _on_nats_disconnect(self):
        print(f"{TAG} Disconnected from NATS")

    async def _on_nats_error(self, e):
        print(f"{TAG} NATS error: {e}")


    # ==================================================================
    # ZeroMQ transport
    # _get_zmq_dealer() → get or create a DEALER socket connected to a local peer
    # _send_zmq() → send a message to a local peer via ZMQ
    # _request_zmq() → send a message and wait for a reply via ZMQ
    # _zmq_receive_loop() → continuously receive messages on the ROUTER socket, dispatch to handler
    # _dispatch() → call the registered handler for a message type, return result

# TODO: make work!
    '''
    def _get_zmq_dealer(self, node_id: str) -> zmq.asyncio.Socket:
        """Get or create a DEALER socket connected to a local peer."""
        if node_id not in self._zmq_dealers:
            peer_ip = self.peers[node_id]["ip"]
            sock = self._zmq_ctx.socket(zmq.DEALER)
            sock.setsockopt_string(zmq.IDENTITY, self.node_id)
            sock.connect(f"tcp://{peer_ip}:{self.ZMQ_PORT}")
            self._zmq_dealers[node_id] = sock
            print(f"{TAG} ZMQ DEALER connected to {node_id} @ {peer_ip}:{self.ZMQ_PORT}")
        return self._zmq_dealers[node_id]


    async def _send_zmq(self, to: str, msg: Message):
        try:
            sock = self._get_zmq_dealer(to)
            payload = json.dumps({**asdict(msg), "reply_to": None}).encode()
            await sock.send_multipart([b"", payload])
        except Exception as e:
            print(f"{TAG} ZMQ send to {to} failed: {e}")


    async def _request_zmq(self, to: str, msg: Message, timeout: float) -> Optional[Message]:
        """
        Send a ZMQ message and wait for a reply.
        Uses a temporary DEALER with a unique reply address embedded in the message.
        """
        try:
            reply_subject = f"zmq.reply.{self.node_id}.{id(msg)}"
            reply_future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._handlers[reply_subject] = lambda p: reply_future.set_result(p) or {}

            sock = self._get_zmq_dealer(to)
            payload = json.dumps({
                **asdict(msg),
                "reply_to": reply_subject,
                "from": self.node_id,
            }).encode()
            await sock.send_multipart([b"", payload])

            await asyncio.wait_for(reply_future, timeout=timeout)
            result = reply_future.result()
            return Message(type="reply", payload=result)
        except asyncio.TimeoutError:
            print(f"{TAG} ZMQ request to {to} timed out")
            return None
        except Exception as e:
            print(f"{TAG} ZMQ request to {to} failed: {e}")
            return None
        finally:
            self._handlers.pop(reply_subject, None)

    async def _zmq_receive_loop(self):
        """Continuously receive messages on the ROUTER socket."""
        while True:
            try:
                frames = await self._zmq_router.recv_multipart()
                # ROUTER frame format: [sender_id, empty, payload]
                if len(frames) < 3:
                    continue
                sender_id = frames[0].decode()
                payload = frames[2]
                data = json.loads(payload)

                reply_to = data.pop("reply_to", None)
                sender = data.pop("from", sender_id)
                msg = Message(type=data["type"], payload=data["payload"])

                result = await self._dispatch(msg)

                # If the sender wants a reply, send it back via NATS
                # (we use NATS for the reply path to avoid needing a reverse ZMQ connection)
                if reply_to and result is not None:
                    await self._send_nats(sender, Message(type=reply_to, payload=result))

            except Exception as e:
                print(f"{TAG} ZMQ receive error: {e}")
                await asyncio.sleep(0.1)'''

    
    async def _dispatch(self, msg: Message):
        handler = self._handlers.get(msg.type)
        if handler:
            result = handler(msg.payload)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        else:
            print(f"{TAG} No handler for message type: {msg.type}")
            return None