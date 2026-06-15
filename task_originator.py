import asyncio
from dataclasses import dataclass
import itertools
import json
from random import random
from time import time
import uuid
from lstm_scoring import load_balanced_score
#from logger import log_latency

# TODO: modify task types and Message class so that PRIVATE_TASK is not a task type, but extra info about the task and task type.
TASK_TYPES          = ["CLASSIFICATION", "TIMESERIES", "PRIVATE_TASK"]
TAG                 = "[ORIG]"
BID_TIMEOUT         = 160
MAX_RETRIES         = 3           # max retry attempts for a deferred task
TASK_INTERVAL       = 15

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None



async def next_task(node, task_cycle) -> tuple:
    """Returns (task_type, retry_attempt). Retry queue takes priority."""
    if node.task_queue:
        task_type, task_id, attempts = node.task_queue.pop()
        print(f"[ORIG] Retrying deferred task: {task_id} of type {task_type} (attempt {attempts+1})")
        return task_type, task_id, attempts + 1
    
    task_id = str(uuid.uuid4())[:8]

    return next(task_cycle), task_id, 0


async def enqueue_retry(node, task_type: str, task_id: str, attempts: int = 0):
    """
    Enqueue a deferred task for retry. If attempts >= MAX_RETRIES, the task is dropped.
    """
    # BUG: Is this attempts variable ever increasing?
    if attempts < MAX_RETRIES:
        node.task_queue.appendleft((task_type, task_id, attempts))
        print(f"{TAG} Task {task_type} re-queued (attempt {attempts+1}/{MAX_RETRIES})")
    else:
        print(f"{TAG} Task {task_type} permanently dropped after {MAX_RETRIES} retries")


def remove_dead_node(node, peer_id: str) -> bool:
    """Remove dead or failed node from peers list."""

    for peer in node.peers:
        if peer[1] == peer_id:
            node.peers.remove(peer)
            break


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
    print(f"{TAG} Asking for bids from peers, for task request {bid_req.payload["task_id"]} ...")
    bids = []

    for lan, peer_id, ip in node.peers:
        bid = None
        try:
            if bid_req.payload["task_type"] != "PRIVATE_TASK":
                bid = await node.bus.global_request((lan, peer_id, ip), bid_req)

            elif bid_req.payload["task_type"] == "PRIVATE_TASK":
                if not ip:
                    print(f"{TAG} Skipping global node...")
                    break

                bid = await node.bus.local_request((lan, peer_id, ip), bid_req)

            # NOTE: Remember that only bids with decision ACCEPT are added to the bids list!!!
            if bid.type == "bid" and \
                bid.payload["task_id"] == bid_req.payload["task_id"] and \
                bid.payload["decision"] == "ACCEPT":

                bids.append(bid.payload)
                
                if len(bids) >= sent_reqests:
                    print(f"{TAG} All nodes responded!")
                    break
                
        except Exception as e: 
            print(f"{TAG} Error in receiving bids:\n{e}")
            continue

    print(f"{TAG} All bids:")
    for bid in bids:
        print(f"Bidder {bid["node_id"]}")

    return bids



async def run_negotiation(node, task_req: Message) -> dict | None:
    print(f"\n{TAG} Running negotiation for task {task_req.payload["task_id"]}...")

    task_req_start = time()   # T1: first TASK_REQUEST sent
    sent = await send_task_request(node, task_req)

    if not sent:
        print(f"{TAG} No nodes acknowledged -- skipping\n")
        return None

    bid_req = Message(
        type="bid_request",
        originator_node=node.id,
        originator_lan=node.lan,
        payload={
            "task_id": task_req.payload["task_id"],
            "task_type": task_req.payload["task_type"]
        }
    )   
    
    bids = await get_bids(node, bid_req, len(sent))
    if not bids:
        print(f"{TAG} No bids received -- skipping\n")
        return None
    
    last_bid_time = time()      # T2: last bid received

    # Pass all bidding node id:s so load_balanced_score sees the full picture
    all_bidders = [bid["node_id"] for bid in bids]
    ranked = sorted(bids,
        key=lambda bid: load_balanced_score(node, bid, all_bidders), reverse=True)
    
    print(f"{TAG} Final ranking: \n")
    for i, bid in enumerate(ranked):
        peer_id = bid["node_id"]
        print(f"{i+1}. {peer_id}: raw score = {bid["score"]}, adjusted score = {bid["adj_score"]}, risk = {bid["risk"]}")

    # NOTE: remove the winner info and only keep the all_bids list + task_id and latency stuff?
    winner = ranked[0]
    winner["task_id"]        = task_req.payload["task_id"]
    winner["all_bids"]       = ranked
    winner["task_req_start"] = task_req_start
    winner["last_bid_time"]  = last_bid_time
    print(f"{TAG} Selected: {winner["node_id"]}")
    #print(f"{TAG} LLM reason: {winner['reason']}") # TODO: add the llm decision info also in the bids!

    print(f"{TAG} winner: {winner}")    # NOTE: for seeing what info the 'winner' includes, can be removed later

    return winner


async def assign_task(node, winner_id: str, task_id: str, task_type: str) -> bool:
    """Assign and send task to the winner node. 
    Returns True on successful assignment, False on failure.
    """

    # Get the lan, node_id and IP of such peer in the node.peers list for which node_id equals peer_id
    winner_info = next(peer for peer in node.peers if peer[1] == winner_id)

    if not winner_info:
        return False
    
    lan, node_id, ip = winner_info
    
    task_assign = Message(
        type="task_assignment",
        originator_node=node.id,
        originator_lan=node.lan,
        payload={
            "task_id": task_id,
            "task_type": task_type,
            "winner_id": winner_id
        })
    
    # Assign task to winner node
    try:
        if task_assign.payload["task_type"] != "PRIVATE_TASK":
            ack = await node.bus.global_request((lan, winner_id, ip), task_assign)

        elif task_assign.payload["task_type"] == "PRIVATE_TASK":
            if not ip:
                print(f"{TAG} Skipping global node...")
            else:
                ack = await node.bus.local_request((lan, winner_id, ip), task_assign)

            if ack.type == "ack":
                print(f"{TAG} Task assigned to node {winner_id}.")
    except Exception as e:
        print(f"{TAG} task assignment error: {e}")
        return False

    return True


def record_assignment(node, peer_id: str):
    """Record that a task was assigned to a peer. 
    peer_id is the id of the peer that was assigned a task."""

    node.assigned_task_counts[peer_id] = node.assigned_task_counts.get(peer_id, 0) + 1
    print(f"{TAG} Assignments: {node.assigned_task_counts[peer_id]}")



async def start(node):
    """Start the Task Originator loop. 
    This will periodically create new tasks and submit them to the MessageBus.
    """
    task_cycle = itertools.cycle(TASK_TYPES)    # NOTE: just for now, for testing.

    while True:
        task_type, task_id, retry_attempt = await next_task(node, task_cycle)

        # TODO: what to do if there are no nodes when starting a task request?

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
        print(f"{TAG} Negotiation results: {negotiation_results}")

        if negotiation_results:
            task_id = negotiation_results["task_id"]
            winner_id = negotiation_results["node_id"]
            all_bids = negotiation_results.pop("all_bids")
            assigned = False

            # TODO: sleep for some time here and then simulate node failure to test node failure handling

            # Go through the sorted all bids list. Attempt to assign the task to the winner node.
            # If winner node is not available anymore, attempt to assign the task to the next node in the list.
            # Continue with this logic until the task is assigned to a node, or until there are no nodes left
            # to assign the task to.
            for candidate in all_bids:
                peer_id = candidate["node_id"]

                if peer_id == winner_id:
                    print(f"{TAG} Assigning task to the winner node {winner_id}...")
                else:
                    print(f"{TAG} Assigning task to node no. {all_bids.index(candidate) + 1} in the ranking list...")
                
                if await assign_task(node, peer_id, task_id, task_type):
                    task_assign_time = time()   # T3: TASK_ASSIGN sent
                    record_assignment(node, peer_id)
                    assigned = True
                    print(f"{TAG} Task assignment successful! Task assigned to node {peer_id}")

                    # Latency breakdown (excludes task execution)
                    t1 = negotiation_results.get("broadcast_start", task_assign_time)
                    t2 = negotiation_results.get("last_bid_time",   task_assign_time)
                    t3 = task_assign_time

                    lat_negotiation = (t2 - t1) * 1000   # broadcast -> last bid
                    lat_assignment  = (t3 - t2) * 1000   # last bid  -> task assign
                    lat_total       = (t3 - t1) * 1000   # broadcast -> task assign

                    # NOTE: We don't have to await this
                    #log_latency(task_type,
                    #    winner_id, negotiation_results["score"],
                    #    negotiation_results["adj_score"],
                    #    lat_negotiation_ms=lat_negotiation,
                    #    lat_assignment_ms=lat_assignment,
                    #    lat_total_ms=lat_total,
                    #    retry_attempt=retry_attempt)
                    break
                else:
                    if all_bids.index(candidate) == 0:
                        print(f"{TAG} Failed to assign task to the winner node." \
                              "Attempting to assign task on another node...")
                    
                    await remove_dead_node(node, peer_id)

            if not assigned:
                print(f"{TAG} Task assignment failed for all nodes.")
                await enqueue_retry(node, task_type, task_id, retry_attempt)

        else:
            print(f"{TAG} No bids for task {task_id}")
            await enqueue_retry(node, task_type, task_id, retry_attempt)

        await asyncio.sleep(TASK_INTERVAL) 