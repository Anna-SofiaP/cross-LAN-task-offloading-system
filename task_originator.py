import asyncio
from dataclasses import dataclass
import itertools
import json
from random import random
from time import time
import uuid


TASK_TYPES          = ["CLASSIFICATION", "TIMESERIES", "PRIVATE_TASK"]
TAG                 = "[ORIG]"
BID_TIMEOUT         = 160

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict


async def run_negotiation(node, task_req: Message) -> dict:
    print(f"\n{TAG} Running negotiation for task {task_req.payload['task_id']}...")

    #broadcast_start = time()   # T1: first TASK_REQUEST sent
    sent = []
    bids = []

    for lan, node_id, ip in node.peers:
        print(f"{TAG} Sending task request to peer {node_id}...\n")
    
        ack_msg = None
        acked = False
        for attempt in range(1, 4):
            try:
                if task_req.payload["task_type"] != "PRIVATE_TASK":
                    ack_msg = await node.bus.global_request((lan, node_id, ip), task_req)
                else:
                    ack_msg = await node.bus.local_request((lan, node_id, ip), task_req)
                    #ack_msg = await node.bus.get_message()
                if ack_msg and ack_msg.payload.get("msg") == "ack":
                    sent.append(node_id)
                    print(f"{TAG} TASK_REQUEST acked by {node_id}\n")
                    acked = True
                    break
            except Exception as e:
                print(f"{TAG} {node_id} attempt {attempt}/3: {e}\n")
                if attempt < 3:
                    await asyncio.sleep(2)
        if not acked:
            print(f"{TAG} Could not reach {node_id} after 3 attempts -- skipping")
    
    if not sent:
        print(f"{TAG} No nodes acknowledged -- skipping\n")
        return None

    return {"results": "Negotiation results (placeholder)"}



async def start(node):
    """Start the Task Originator loop. This will periodically create new tasks and submit them to the MessageBus."""
    task_cycle = itertools.cycle(TASK_TYPES)    # NOTE: just for now, for testing.

    while True:
        await asyncio.sleep(10)  # Simulate delay

        # For testing, we just create random tasks
        task_id = str(uuid.uuid4())[:8]
        task_type = next(task_cycle)

        print(f"\n{TAG} {'='*52}")
        print(f"{TAG} New task: task id={task_id}, type={task_type}")
        print(f"{TAG} {'='*52}")

        task_req = Message(
            type="task_request",
            originator_node=node.id,
            originator_lan=node.lan,
            payload={
                "task_id": task_id,
                "task_type": task_type
            }
        )

        negotiation_results = await run_negotiation(node, task_req)
        #negotiation_results = {"results": "Negotiation results (placeholder)"}
        print(f"{TAG} Negotiation results: {negotiation_results}")