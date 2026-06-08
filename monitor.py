"""
monitor.py — Heartbeat and peer management

Plain functions that run as background loops.
start() is called once at startup and runs forever.
"""

import asyncio
from node import Node

TAG = "[MONITOR]"
HEARTBEAT_INTERVAL = 20


#async def start(node: Node):
#    """Start all monitor loops concurrently."""
#    print(f"{TAG} Starting for node {node.id}")
#    await asyncio.gather(
#        _heartbeat_loop(node),
##        _stale_peer_cleanup_loop(node),
#    )


async def heartbeat_loop(node: Node):
    """Broadcast this node's presence and status to the whole cluster."""
    print(f"{TAG} Sending heartbeat signal to peers every {HEARTBEAT_INTERVAL} seconds.")
    while True:
        try:
            await node.bus.publish_heartbeat(lan=node.lan)
            print(
                f"{TAG} Heartbeat sent. "
                f"Known peers: {list(node.bus.peers.keys())}"
            )
        except Exception as e:
            print(f"{TAG} Heartbeat error: {e}")
        print(f"{TAG} Sleeping for {HEARTBEAT_INTERVAL} seconds...")
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        print(f"{TAG} Woke up!")


'''async def _stale_peer_cleanup_loop(node: Node):
    """Remove peers that have stopped sending heartbeats."""
    while True:
        await asyncio.sleep(node.bus.heartbeat_interval)
        now = asyncio.get_event_loop().time()
        stale = [
            node_id
            for node_id, info in node.bus.peers.items()
            if now - info["last_seen"] > node.bus.peer_stale_timeout
        ]
        for node_id in stale:
            del node.bus.peers[node_id]
            sock = node.bus._zmq_dealers.pop(node_id, None)
            if sock:
                sock.close()
            print(f"{TAG} Peer removed (stale): {node_id}")'''