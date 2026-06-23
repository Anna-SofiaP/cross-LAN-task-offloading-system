import asyncio
from dataclasses import dataclass
import itertools
from random import random
from time import time
import uuid
#from monitor import task_monitor_and_failover_loop
from lstm_scoring import load_balanced_score
from task_assign import assign_task, enqueue_retry, record_assignment
#from logger import log_latency

# TODO: modify task types and Message class so that PRIVATE_TASK is not a task type, but extra info about the task and task type.
TASK_TYPES              = ["CLASSIFICATION", "TIMESERIES", "PRIVATE_TASK"]
TAG                     = "[ORIG]"
BID_TIMEOUT             = 160
TASK_INTERVAL           = 15
FAILOVER_TIMEOUT        = 60
HEARTBEAT_INTERVAL      = 20


@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None



async def next_task(node, task_cycle) -> tuple[str, str, int]:
    """Returns (task_type, retry_attempt). Retry queue takes priority."""
    if node.task_queue:
        task_type, task_id, attempts = node.task_queue.pop()
        print(f"[ORIG] Retrying deferred task: {task_id} of type {task_type} (attempt {attempts+1})")
        return task_type, task_id, attempts + 1
    
    task_id = str(uuid.uuid4())[:8]

    return next(task_cycle), task_id, 0


def remove_dead_node(node, peer_id: str) -> bool:
    """Remove dead or failed node from message bus peers list and node peers list."""

    for peer in node.peers:
        if peer[1] == peer_id:
            node.peers.remove(peer)
            break

    for peer in node.bus.peers:
        if peer["node_id"] == peer_id:
            node.bus.peers.remove(peer)
            break

    print(f"{TAG} Peer {peer[1]} (lan: {peer[0]}) removed from peers list." +
      f"    Know peers of node: {node.peers}" +
      f"    Known peers of messagebus: {node.bus.peers}")


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
                    print(f"{TAG} Sending to: {lan}, {peer_id}, {ip}")
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
    print(f"{TAG} Asking for bids from peers, for task {bid_req.payload["task_id"]} ...")
    bids = []

    for lan, peer_id, ip in node.peers:
        bid = None
        try:
            if bid_req.payload["task_type"] != "PRIVATE_TASK":
                bid = await node.bus.global_request((lan, peer_id, ip), bid_req)

            elif bid_req.payload["task_type"] == "PRIVATE_TASK":
                print(f"{TAG} Sending to: {lan}, {peer_id}, {ip}")
                if not ip:
                    print(f"{TAG} Skipping global node...")
                    continue

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



async def run_negotiation(node, task_id: str, task_type: str) -> dict | None:
    task_req = Message(
            type="task_request",
            originator_node=node.id,
            originator_lan=node.lan,
            payload={
                "task_id": task_id,
                "task_type": task_type,
                #"in_data_sens_level": "??",
                #"out_data_sens_level": "??"
            }
        )

    #task_id = task_req.payload["task_id"]
    #task_type = task_req.payload["task_type"]
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
    #print(f"{TAG} LLM reason: {winner['reason']}")

    print(f"{TAG} Ranking: {results}")

    return results



async def task_monitor_and_failover_loop(node, task_id: str, task_type: str, peer, retry_attempt: int):
    """ Monitors assigned node. On failure:
      - Marks node dead (only ONE thread handles each failure)
      - Re-negotiates with remaining LIVE nodes
      - Assigns same task to new winner (task completion guarantee)
    """
    peer_id = peer["node_id"]
    node_failure = False

    print(f"{TAG} Failover monitoring started for {peer_id}, task {task_id}")

    for peer in node.bus.peers:
        node_id = peer["node_id"]
        last_seen = peer["last_seen"]

        if node_id == peer_id:
            while True:
                asyncio.sleep(HEARTBEAT_INTERVAL)
                now = asyncio.get_event_loop().time()

                if now - last_seen > FAILOVER_TIMEOUT:
                    print(f"{TAG} Peer has not been sending heartbeat signal for >={FAILOVER_TIMEOUT} seconds.")
                    remove_dead_node(node, peer_id)
                    node_failure = True
                    break

    if len(node.peers) == 0:
        print(f"{TAG} No remaining peers -- re-queueing task")
        enqueue_retry(node, task_type, task_id, retry_attempt)
        # No reason to run new negotiation at this point.
        return

    if node_failure:
        # Run new negotiation with the other live peers
        print(f"{TAG} Failover with {len(node.peers)} node(s)")
        new_results = run_negotiation(node)

        if new_results:
            all_bids = new_results.pop("all_bids")
            assigned = False

            for candidate in all_bids:
                candidate_id = candidate["node_id"]

                if assign_task(node, candidate_id, task_id, task_type):
                    record_assignment(node, candidate_id)

                    print(f"\n{TAG} FAILOVER -> {candidate["node_id"]}")
                    print(f"{TAG} Score: {candidate["score"]:.4f}")
                    #print(f"{TAG} Reason: {candidate["reason"]}")

                    #winner = candidate
                    assigned = True
                    break
                else:
                    remove_dead_node(node, candidate_id)
            if not assigned:
                print(f"{TAG} All failover candidates failed -- re-queueing task")
                enqueue_retry(node, task_type, task_id, retry_attempt)
        else:
            print(f"{TAG} No bids during failover -- re-queueing task")
            enqueue_retry(node, task_type, task_id, retry_attempt)


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


        negotiation_results = await run_negotiation(node, task_id, task_type)
        print(f"{TAG} Negotiation results: {negotiation_results}")

        # TODO: For getting the task result, add a loop that queries the agent to get the results

        if negotiation_results:
            #task_type = negotiation_results["task_type"]
            #winner_id = negotiation_results["node_id"]
            all_bids = negotiation_results.pop("all_bids")
            assigned = False

            # Go through the sorted all bids list. Attempt to assign the task to the winner node.
            # If winner node is not available anymore, attempt to assign the task to the next node in the list.
            # Continue with this logic until the task is assigned to a node, or until there are no nodes left
            # to assign the task to.
            for candidate in all_bids:
                peer_id = candidate["node_id"]

                print(f"{TAG} Assigning task to node {peer_id}...")

                task_assign_time = time()   # T3: task assignment sent

                await asyncio.sleep(5) # NOTE: for testing!

                if await assign_task(node, peer_id, task_id, task_type):
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
                    asyncio.create_task(task_monitor_and_failover_loop(node, task_id, task_type, candidate, retry_attempt))
                    break
                else:
                    if all_bids.index(candidate) == 0:
                        print(f"{TAG} Failed to assign task to the winner node. -- next")

                    remove_dead_node(node, peer_id)

            if not assigned:
                print(f"{TAG} Task assignment failed for all nodes.")
                await enqueue_retry(node, task_type, task_id, retry_attempt)

        else:
            print(f"{TAG} No bids for task {task_id}")
            await enqueue_retry(node, task_type, task_id, retry_attempt)

        await asyncio.sleep(TASK_INTERVAL) 