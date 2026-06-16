import asyncio
#from dataclasses import dataclass
import itertools
import json
from random import random
from time import time
import uuid
import threading
from lstm_scoring import load_balanced_score
from node import Message
from task_assign import do_task_assignment, enqueue_retry
#from logger import log_latency

# TODO: modify task types and Message class so that PRIVATE_TASK is not a task type, but extra info about the task and task type.
TASK_TYPES          = ["CLASSIFICATION", "TIMESERIES", "PRIVATE_TASK"]
TAG                 = "[ORIG]"
BID_TIMEOUT         = 160
TASK_INTERVAL       = 15


#@dataclass
#class Message:
#    type: str
#    originator_node: str
#    originator_lan: str
#    payload: dict = None



async def next_task(node, task_cycle) -> tuple[str, str, int]:
    """Returns (task_type, retry_attempt). Retry queue takes priority."""
    if node.task_queue:
        task_type, task_id, attempts = node.task_queue.pop()
        print(f"[ORIG] Retrying deferred task: {task_id} of type {task_type} (attempt {attempts+1})")
        return task_type, task_id, attempts + 1
    
    task_id = str(uuid.uuid4())[:8]

    return next(task_cycle), task_id, 0


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
    task_id = task_req.payload["task_id"]
    task_type = task_req.payload["task_type"]
    print(f"\n{TAG} Running negotiation for task {task_id}...")

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
            "task_id": task_id,
            "task_type": task_type
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
    #winner = ranked[0]
    results                   = {}
    results["task_id"]        = task_id
    results["task_type"]      = task_type
    results["all_bids"]       = ranked
    results["task_req_start"] = task_req_start
    results["last_bid_time"]  = last_bid_time

    print(f"{TAG} Selected winner: {ranked[0]["node_id"]}")
    #print(f"{TAG} LLM reason: {winner['reason']}") # TODO: add the llm decision info also in the bids!

    print(f"{TAG} Ranking: {results}")

    return results



def send_task_and_get_result_thread(args):
    task_exec_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(task_exec_loop)

    task_exec_loop.run_until_complete(do_task_assignment(args))
    task_exec_loop.close()


async def start(node):
    """Start the Task Originator loop. 
    This will periodically create new tasks and submit them to the MessageBus.
    """
    task_cycle = itertools.cycle(TASK_TYPES)    # NOTE: just for now, for testing.

    while True:
        task_type, task_id, retry_attempt = await next_task(node, task_cycle)

        if not node.peers:
            print(f"{TAG} No peers currently available. Wating for peers to join...")
            await asyncio.sleep(TASK_INTERVAL)
            continue


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
            #node.tasks_to_be_assigned.append(negotiation_results)
            thread = threading.Thread(target=send_task_and_get_result_thread, 
                                      args=(node, task_id, task_type, retry_attempt, negotiation_results),
                                      daemon=True,
                                      name=f"exec_task_{task_id}")
            node.task_threads_and_results[task_id]["thread"] = thread
            thread.start()

        else:
            print(f"{TAG} No bids for task {task_id}")
            await enqueue_retry(node, task_type, task_id, retry_attempt)

        await asyncio.sleep(TASK_INTERVAL) 