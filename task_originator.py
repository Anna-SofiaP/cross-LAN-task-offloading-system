import asyncio
from dataclasses import dataclass
import itertools
import json
from random import random
from time import time
import uuid
from lstm_scoring import load_balanced_score

# TODO: modify task types and Message class so that PRIVATE_TASK is not a task type, but extra info about the task and task type.
TASK_TYPES          = ["CLASSIFICATION", "TIMESERIES", "PRIVATE_TASK"]
TAG                 = "[ORIG]"
BID_TIMEOUT         = 160

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


async def send_task_request(node, task_req) -> list:
    sent = []

    for lan, peer_id, ip in node.peers:
        print(f"{TAG} Sending task request to peer {peer_id}...")
    
        ack_msg = None
        acked = False
        for attempt in range(1, 4):
            try:
                if task_req.payload["task_type"] != "PRIVATE_TASK":
                    ack_msg = await node.bus.global_request((lan, peer_id, ip), task_req)

                elif task_req.payload["task_type"] == "PRIVATE_TASK":
                    if not ip:
                        print(f"{TAG} Skipping global node...")
                        break

                    ack_msg = await node.bus.local_request((lan, peer_id, ip), task_req)

                if ack_msg and ack_msg.type == "ack":
                    sent.append(peer_id)
                    print(f"{TAG} TASK_REQUEST acked by {peer_id}")
                    acked = True
                    break
            except Exception as e:
                print(f"{TAG} {peer_id} attempt {attempt}/3: {e}")
                if attempt < 3:
                    await asyncio.sleep(3)

        if not acked:
            print(f"{TAG} Could not reach {peer_id} after 3 attempts -- skipping\n")
    
    return sent



async def get_bids(node, bid_req: Message, sent_reqests: int):
    print(f"{TAG} Asking for bids from peers, for task request {bid_req.payload['task_id']} ...")
    bids = []

    for lan, peer_id, ip in node.peers:
        bid = None
        try:
            if bid_req.payload['task_type'] != "PRIVATE_TASK":
                bid = await node.bus.global_request((lan, peer_id, ip), bid_req)

            elif bid_req.payload["task_type"] == "PRIVATE_TASK":
                if not ip:
                    print(f"{TAG} Skipping global node...")
                    break

                bid = await node.bus.local_request((lan, peer_id, ip), bid_req)

            # NOTE: Remember that only bids with decision ACCEPT are added to the bids list!!!
            if bid.type == "bid" and bid.payload["task_id"] == bid_req.payload["task_id"]:
                #bid["_key"] = f"{bid['node_ip']}:{bid['peer_id']}"
                #bid_info = {f"bid_info": bid.payload}
                #bids.append(bid_info)
                bids.append(bid.payload)

                # TODO: bid should be a Message, check format!
        #        print(f"{TAG} Bid from {peer_id}  "
        #              f"raw={bid.payload["score"]:.4f}  adj={load_balanced_score(node, peer_id, bid_info):.4f}")
                
                if len(bids) >= sent_reqests:
                    print(f"{TAG} All nodes responded!")
                    break
                
        except Exception as e: 
            print(f"{TAG} Error in receiving bids:\n{e}")
            continue

    print(f"{TAG} All bids:")
    for bid in bids:
        # TODO: add print for decision: ACCEPTED/REJECTED
        print(f"Bidder {bid["node_id"]}")

    return bids



async def run_negotiation(node, task_req: Message) -> dict:
    print(f"\n{TAG} Running negotiation for task {task_req.payload['task_id']}...")

    #broadcast_start = time()   # T1: first TASK_REQUEST sent
    sent = await send_task_request(node, task_req)

    if not sent:
        print(f"{TAG} No nodes acknowledged -- skipping\n")
        return {"results": None}

    bid_req = Message(
        type="bid_request",
        originator_node=node.id,
        originator_lan=node.lan,
        payload={
            "task_id": task_req.payload['task_id'],
            "task_type": task_req.payload['task_type']
        }
    )   
    
    bids = await get_bids(node, bid_req, len(sent))
    if not bids:
        print(f"{TAG} No bids received -- skipping\n")
        return {"results": None}

    # Pass all bidding node id:s so load_balanced_score sees the full picture
    all_bidders = [bid["node_id"] for bid in bids]
    ranked = sorted(bids,
        key=lambda bid: load_balanced_score(node, bid, all_bidders), reverse=True)
    
    print(f"{TAG} Bids ranked:" \
          f"--> {ranked}")

    return {"results": bids}



async def start(node):
    """Start the Task Originator loop. This will periodically create new tasks and submit them to the MessageBus."""
    task_cycle = itertools.cycle(TASK_TYPES)    # NOTE: just for now, for testing.

    while True:
        await asyncio.sleep(10)  # NOTE: Simulate delay, remove later

        # NOTE: For testing, we just create random tasks
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
        print(f"{TAG} Negotiation results: {negotiation_results["results"]}")