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
     The 'type' field determines how the message is handled by the receiving node.

     payload: can contain any data relevant to the message type.
     type: can be for example 'task_req_ack', 'bid'
     originator_node: the node_id of the sender, used for routing replies.
     originator_lan: the LAN of the sender, used for routing and debugging."""
    
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

    def __init__(self, node_id: str, nats_url: str, lan: str, heartbeat_interval=30):
        self.node_id = node_id
        self.nats_url = nats_url
        self.lan = lan
        self.local_ip = get_local_ip()

        self.heartbeat_interval = heartbeat_interval

        # Peer registry: node_id → {"ip": ..., "status": ..., "last_seen": ..., "local": bool}
        self.peers: dict[str, dict] = {} #NOTE: Is this needed? Change to a list of tuples instead?

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


    '''
    async def send(self, to: str, msg: Message):
        """Send a fire-and-forget message to a node. Transport chosen automatically."""
        if self._is_local(to):
            await self._send_zmq(to, msg)
        else:
            await self._pub_nats(to, msg)'''

    '''
    async def request(self, to: tuple, msg: Message, timeout: float = 3.0) -> Message:
        """Send a message and wait for a reply. Transport chosen automatically."""

        lan, topic, ip = to
        print(f"{TAG} Sending request to node {topic} in LAN {lan}")

        if self._is_local(lan) and ip is not None or msg.payload["task_type"] == "PRIVATE_TASK":
            print(f"{TAG} Using ZeroMQ for local request")
            await self._send_zmq(ip, msg, timeout)
        else:
            print(f"{TAG} Using NATS for remote request")
            result = await self._request_nats(topic, msg, timeout)'''
    

    # TODO: merge global_request and local_request to one request function? And do decision about messaging there!
    async def global_request(self, to: tuple, msg: Message, timeout: float = 3.0) -> Message:
        lan, topic, ip = to
        print(f"{TAG} Sending request to node {topic} in LAN {lan}\n")
        return await self._request_nats(topic, msg, timeout)
    
    async def local_request(self, to: tuple, msg: Message) -> Message:
        lan, topic, ip = to
        if not ip:
            return
        print(f"{TAG} Sending request to node {topic} in LAN {lan}\n")
        return await self._send_zmq(ip, msg)


    async def publish_heartbeat(self, lan: str):
        """Broadcast a heartbeat to the whole cluster via NATS."""
        print(f"{TAG} Publishing heartbeat signal...\n")
        if self.nc is None:
            return
        await self.nc.publish(TOPIC_HEARTBEAT, json.dumps({
            "node_id": self.node_id,
            "lan": lan,
            "ip": self.local_ip,
        }).encode())


    '''
    async def get_local_message(self) -> Message:
        return await self._get_zmq_message(self)'''


    # ==================================================================
    # CONNECTTION MANAGEMENT
    # ===============================================================

    async def connect(self):
        """Connect to NATS server and bind to ZMQ port."""
        await self._connect_nats()
        #await self._connect_to_zmq_sockets()

        print(f"{TAG} Setting up ZeroMQ REP socket for incoming requests...")
        self.rep_sock = self.ctx.socket(zmq.REP)
        self.rep_sock.bind(f"tcp://0.0.0.0:{self.ZMQ_PORT}")

        #asyncio.create_task(self._zmq_listen_loop())

        print(f"{TAG} Node {self.node_id} connected. Local IP: {self.local_ip}")


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
        await self.nc.subscribe(f"nodes.{self.node_id}", cb=self._on_nats_message)
        # Subscribe to cluster-wide heartbeats
        await self.nc.subscribe(TOPIC_HEARTBEAT, cb=self._on_heartbeat)

        print(f"\n{TAG} Subscribed to nodes.{self.node_id} and {TOPIC_HEARTBEAT}\n")


    async def _connect_to_zmq_sockets(self):
        """Connect to the local ZeroMQ socket for direct peer communication."""
        #"""Start a ZMQ ROUTER socket to receive direct messages from local peers."""
        #self._zmq_router = self._zmq_ctx.socket(zmq.ROUTER)
        #self._zmq_router.bind(f"tcp://0.0.0.0:{self.ZMQ_PORT}")
        #asyncio.create_task(self._zmq_receive_loop())
        #print(f"{TAG} ZeroMQ listener on port {self.ZMQ_PORT}")

        # TODO: these down below are for a pull socket. Make work if needed!
        #self.pull_sock.setsockopt(zmq.RCVTIMEO, int(BID_TIMEOUT*1000))
        #self.pull_sock.bind(f"tcp://0.0.0.0:{self.ZMQ_PORT}")

        # REQ-REP sockets


    # ==================================================================
    # PEER MANAGEMENT
    # =================================================================

    def _is_local(self, lan: str) -> bool:
        """Check if the node in question is on the same LAN (i.e. we have a direct ZMQ connection)"""
        return lan is not None and self.lan == lan


    async def _update_peer(self, node_id: str, lan: str, ip: str):
        """Add a new peer to the registry or update an existing one. 
        If it is a new peer, call the registered callbacks.
        
        Currently there is only one callback registered by the Node class to just print the new peer info.
        """

        #is_local = same_subnet(self.local_ip, ip)
        existed = node_id in self.peers
        self.peers[node_id] = {
            "ip": ip,
            "lan": lan,
            "last_seen": asyncio.get_event_loop().time(),
        #    "local": is_local,
        }
        if not existed:
            transport = "ZeroMQ (direct)" if lan == self.lan else "NATS (via broker)"
            print(f"{TAG} New peer discovered: {node_id} @ {ip} — transport: {transport}\n")
            for cb in self._peer_callbacks:
                await cb(node_id, self.peers[node_id])

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
            print(f"{TAG} Sending NATS request to {topic} with payload: {msg.payload}")
            reply = await self.nc.request(
                f"nodes.{topic}",
                json.dumps(asdict(msg)).encode(),
                timeout=timeout,
            )
            print(f"\n{TAG} Received NATS reply from {topic}: {reply.data.decode()}\n")

            # BUG: what should the originator_node and originator_lan be in the reply? Currently we just set them to the same as the request, but maybe they should be the topic's node_id and LAN?
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
            print(f"{TAG} Received NATS message of type {msg.type} from {msg.originator_node}\n")
            response = await self._dispatch(msg)
            if raw_msg.reply and response is not None:
                await self.nc.publish(raw_msg.reply, json.dumps(response).encode())
        except Exception as e:
            print(f"{TAG} Error handling NATS message: {e}")
            

    async def _on_heartbeat(self, raw_msg):
        """Handle an incoming heartbeat from any node in the cluster."""
        try:
            data = json.loads(raw_msg.data)
            node_id = data["node_id"]
            if node_id == self.node_id:
                return  # ignore own heartbeat
            
            print(f"{TAG} Received a heartbeat signal from a peer!\n")
            
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
        return self._zmq_dealers[node_id]'''

    
    async def _send_zmq(self, ip: str, msg: Message) -> Message:
        """Send a message through the ZMQ request socket."""
        print(f"{TAG} Sending ZMQ message to {ip}...")
        try:
            req_sock = self.ctx.socket(zmq.REQ)
            req_sock.setsockopt(zmq.RCVTIMEO, 5000)
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
            #msg = Message(msg.type, msg.originator_lan, msg.originator_node, msg.payload)
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

                await self.rep_sock.send_string(json.dumps(reply))

            except Exception as e:
                print(f"{TAG} ZMQ listen loop error: {e}")
                await self.rep_sock.send_string(json.dumps({"msg": "error"}))
                # always send something back or the REQ side hangs


    '''
    async def _request_zmq(self, ip: str, msg: Message, timeout: float) -> Optional[Message]:
        try:
            self.req_sock.connect(f"tcp://{ip}:{self.ZMQ_PORT}")

            req = json.dumps(dict(type=msg.type,
                                  originator_node=msg.originator_node, 
                                  originator_lan=msg.originator_lan),
                                  task_id=msg.payload.get("task_id"), 
                                  task_type=msg.payload.get("task_type")).encode()

            self.req_sock.send_string(req)
            ack = json.loads(self.req_sock.recv_string())

            self.req_sock.close()

            # BUG: change originator_node and originator_lan in the reply to be the actual sender's info instead of just echoing the request's originator info
            return Message("ack", self.node_id, self.lan, payload=json.loads(ack.get("payload", "{}")))
        except Exception as e:
            print(f"{TAG} ZMQ request to {ip} failed: {e}")
            raise'''
    
    '''
    async def _get_zmq_message(self, msg) -> Message:
        try:
            raw = await self.rep_sock.recv_string()
            msg = json.loads(raw)
            msg = Message(**msg)
            print(f"{TAG} Received ZMQ message of type {msg.type} from {msg.originator_node}")
            return msg
        except Exception as e:
            # TODO: handle error situation
            print(f"{TAG} Error handling ZMQ message: {e}")
            return None'''


    '''
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

    
    async def _dispatch(self, msg: Message) -> Message:
        print(f"{TAG} Dispatching message of type {msg.type} to handler...\n")
        handler = self._handlers.get(msg.type)
        if handler:
            result = handler(msg.payload)
            if asyncio.iscoroutine(result):
                result = await result
            response = Message(type=result["msg"], originator_node=self.node_id, originator_lan=self.lan)
            return response
        else:
            print(f"{TAG} No handler for message type: {msg.type}\n")
            return None