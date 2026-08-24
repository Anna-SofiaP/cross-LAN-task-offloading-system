import asyncio
from dataclasses import dataclass
import itertools
import random
from time import time
import uuid
from task_assign import assign_task, enqueue_retry, record_assignment
from offloading_decision_making import load_balanced_score, reliability_and_privacy_assessment, make_final_offloading_decision
#from logger import log_latency

TASK_TYPES              = ["CLASSIFICATION", "TIMESERIES", "CV_INFERENCE"]
DATA_PRIVACY_LEVELS     = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]
TASK_PRIORITY_LEVELS    = ["HIGH", "MEDIUM", "LOW"]
# TODO: Instead of task priority levels, should we have task deadlines? 
#       For example, a task can be "immediate", "as soon as possible", or "whenever". 
#       This would be more flexible and specific than just having a priority level. 
#       The node can then decide if it can complete the task within the deadline based on its current load and predicted future load.

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
    """Returns the new task. Retry queue takes priority."""
    in_data_privacy_level = random.choice(DATA_PRIVACY_LEVELS)
    out_data_privacy_level = random.choice(DATA_PRIVACY_LEVELS)
    task_priority = random.choice(TASK_PRIORITY_LEVELS)

# NOTE: These are one way of giving the task attributes to the system
#    in_data_privacy_level = input("Enter input data privacy level (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED): ")
#    out_data_privacy_level = input("Enter output data privacy level (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED): ")
#    task_priority = input("Enter task priority (HIGH, MEDIUM, LOW): ")

    if node.task_queue:
        task_type, task_id, in_data_privacy_lvl, out_data_privacy_lvl, t_priority, attempts = node.task_queue.pop()
        print(f"[ORIG] Retrying deferred task: {task_id} of type {task_type} (attempt {attempts+1})")
        return task_type, task_id, in_data_privacy_lvl, out_data_privacy_lvl, t_priority, attempts + 1
    
    task_id = str(uuid.uuid4())[:8]

    return next(task_cycle), task_id, in_data_privacy_level, out_data_privacy_level, task_priority, 0


def remove_dead_node(node, peer_id: str) -> bool:
    """Remove dead or failed node from node peers list and message bus peers list."""

    for peer in node.peers:
        if peer[1] == peer_id:
            node.peers.remove(peer)
            print(f"{TAG} Peer {peer[1]} (lan: {peer[0]}) removed from peers list." +
                f"    Know peers of node: {node.peers}" +
                f"    Known peers of messagebus: {node.bus.peers}")
            break

    for peer in node.bus.peers:
        if peer["node_id"] == peer_id:
            node.bus.peers.remove(peer)
            break


async def send_task_request(node, task_req) -> list:
    sent = []

    for lan, peer_id, ip in node.peers:
        print(f"{TAG} Sending task request to peer {peer_id}...")
    
        ack_msg = None
        acked = False
        for attempt in range(1, 4):
            try:
                ack_msg = await node.bus.request((lan, peer_id, ip), task_req)

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
    print(f"{TAG} Requesting for bids from peers...")
    bids = []

    for lan, peer_id, ip in node.peers:
        bid = None
        try:
            bid = await node.bus.request((lan, peer_id, ip), bid_req)

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
        print(f"\t{bid["node_id"]}")

    return bids



async def run_negotiation(node, task_id: str, task_type: str, in_data_privacy_lvl: str, 
                          out_data_privacy_lvl: str, task_priority: str) -> dict | None:
    """Run negotiation for a task."""

    task_req = Message(
            type="task_request",
            originator_node=node.id,
            originator_lan=node.lan,
            payload={
                "task_id": task_id,
                "task_type": task_type,
            }
        )

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
            "task_type": task_type,
            "in_data_privacy_lvl": in_data_privacy_lvl,
            "out_data_privacy_lvl": out_data_privacy_lvl,
            "task_priority": task_priority,
            #TODO: what to do if the data is on the target node already and nothing needs to be sent? Or should we skip that option?
        }
    )   
    
    bids = await get_bids(node, bid_req, len(sent))
    if not bids:
        print(f"{TAG} No bids received -- skipping\n")
        return None
    
    last_bid_time = time()      # T2: last bid received

    # Pass all bidding node id:s so load_balanced_score sees the full picture
    all_bidders = [bid["node_id"] for bid in bids]


    for bid in bids:
        bid["adj_score"] = load_balanced_score(node, bid, all_bidders)
        bid["pr_score"] = reliability_and_privacy_assessment(bid, in_data_privacy_lvl, out_data_privacy_lvl, task_priority, node.lan)

    # Discard all nodes with joint privacy-reliability score of value -2
    valid_bids = [bid for bid in bids if bid["pr_score"] != -2]

    final_ranking = sorted(valid_bids,
        key=lambda bid: make_final_offloading_decision(bid, valid_bids), reverse=True)
    
    print(f"\n{TAG} Final ranking: \n")

    for i, bid in enumerate(final_ranking):
        peer_id = bid["node_id"]
        print(f"{i+1}. {peer_id}: raw score={bid["score"]}, adj. score={bid["adj_score"]}, "
              f"pr score={bid["pr_score"]}, final score (WP)={bid["w_product"]}")

    results                   = {}
    results["task_id"]        = task_id
    results["task_type"]      = task_type
    results["all_bids"]       = final_ranking
    results["task_req_start"] = task_req_start
    results["last_bid_time"]  = last_bid_time

    #print(f"{TAG} Selected winner: {ranked[0]["node_id"]}")

    #print(f"{TAG} Ranking: {results}")

    return results



async def task_monitor_and_failover_loop(node, task_id: str, task_type: str, peer, retry_attempt: int):
    """ Monitors assigned node. On failure:
      - Marks node dead (only ONE thread handles each failure)
      - Re-negotiates with remaining LIVE nodes
      - Assigns same task to new winner (task completion guarantee)
    """
    peer_id = peer["node_id"]
    node_failure = False
    task_result = None
    result_request_attempts = 0

    # Wait for some time before starting to monitor the assigned node for task completion. 
    # This is to give the assigned node some time to execute the task.
    #NOTE: This value can be changed to something else. Doesn't have to be the same as the HEARTBEAT_INTERVAL.
    await asyncio.sleep(HEARTBEAT_INTERVAL)

    task_result_req = Message(
                    type="task_result_request",
                    originator_node=node.id,
                    originator_lan=node.lan,
                    payload={
                        "task_id": task_id,
                        "task_type": task_type,
                    }
                )

    print(f"{TAG} Failover monitoring started for {peer_id}, task {task_id}")

    # Get communication and last seen information about the peer
    peer_info = next((peer for peer in node.peers if peer[1] == peer_id), None)
    peer_last_seen = next((p["last_seen"] for p in node.bus.peers if p["node_id"] == peer_id), None)
    lan, p_id, ip = peer_info if peer_info else (None, None, None)

    while node_failure == False or not task_result:

        if not peer_info:
            print(f"{TAG} Peer {peer_id} not found in peers list -- re-negotiating task assignment")
            # NOTE: Peer not found in peers list can be due to many reasons, not only peer failure.
            node_failure = True
            break

        try:
            response = await node.bus.request((lan, p_id, ip), task_result_req)

            if response.type == "result":
                task_result = response.payload
                print(f"{TAG} Task {task_id} completed successfully by {peer_id}.")
                print(f"\n{25*'='}")
                print(f"{TAG} Task result: {task_result}")
                print(f"{25*'='}\n")
                print(f"{TAG} Exiting failover monitoring")
                return
            elif response.type == "no_result":
                print(f"{TAG} Peer {peer_id} has not completed task {task_id} yet. Continuing to monitor...")
                result_request_attempts = 0

        except Exception as e:
            print(f"{TAG} Error in requesting task result: {e}")
            result_request_attempts += 1

        if result_request_attempts >= 3:
            print(f"{TAG} Peer {peer_id} failed to respond after 3 attempts -- re-negotiating task assignment")
            node_failure = True
            break

        await asyncio.sleep(HEARTBEAT_INTERVAL)
        now = asyncio.get_event_loop().time()

        if now - peer_last_seen > FAILOVER_TIMEOUT:
            print(f"{TAG} Peer has not been sending heartbeat signal for >={FAILOVER_TIMEOUT} seconds.")
            node_failure = True

    if node_failure:
        # In case of node failure, remove the dead node from the node.peers list and message bus peers list
        remove_dead_node(node, peer_id)

        if len(node.peers) == 0:
            print(f"{TAG} No remaining peers -- re-queueing task")
            enqueue_retry(node, task_type, task_id, retry_attempt)
            # No reason to run new negotiation at this point.
            return

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
        task_type, task_id, \
        in_data_privacy_lvl, \
        out_data_privacy_lvl, \
        task_priority, \
        retry_attempt = await next_task(node, task_cycle)

        if not node.peers:
            print(f"{TAG} No peers currently available. Wating for peers to join...")
            await asyncio.sleep(TASK_INTERVAL)
            continue

        print(f"\n{'='*52}")
        print(f"{TAG}   New task: task id={task_id}, type={task_type}")
        print(f"        in data privacy lvl={in_data_privacy_lvl}, out data privacy lvl={out_data_privacy_lvl}")
        print(f"        task priority={task_priority}")
        print(f"{'='*52}")


        negotiation_results = await run_negotiation(node, task_id, task_type, 
                                                    in_data_privacy_lvl, out_data_privacy_lvl, task_priority)
        
        print(f"{TAG} Negotiation results: {negotiation_results}")

        if negotiation_results:
            all_bids = negotiation_results.pop("all_bids")
            assigned = False

            # Go through the sorted all bids list. Attempt to assign the task to the winner node.
            # If winner node is not available anymore, attempt to assign the task to the next node in the list.
            # Continue with this logic until the task is assigned to a node, or until there are no nodes left
            # to assign the task to.
            for candidate in all_bids:
                peer_id = candidate["node_id"]

                #print(f"{TAG} Assigning task to node {peer_id}...")

                task_assign_time = time()   # T3: task assignment sent

                if await assign_task(node, peer_id, task_id, task_type):
                    record_assignment(node, peer_id)
                    assigned = True

                    print(f"{TAG} Task assignment successful!\n")

                    # Latency breakdown (excludes task execution)
                    t1 = negotiation_results.get("broadcast_start", task_assign_time)
                    t2 = negotiation_results.get("last_bid_time",   task_assign_time)
                    t3 = task_assign_time

                    lat_negotiation = (t2 - t1) * 1000   # broadcast -> last bid
                    lat_assignment  = (t3 - t2) * 1000   # last bid  -> task assign
                    lat_total       = (t3 - t1) * 1000   # broadcast -> task assign

                    # TODO: do we have to await this?
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