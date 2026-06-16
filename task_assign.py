from time import time

from node import Message

TAG = "[ASSIGN]"
MAX_RETRIES = 3           # max retry attempts for a deferred task


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


async def send_task_to_be_executed_and_receive_result(node, winner_id: str, task_id: str, task_type: str) -> dict:
    """Assign and send task to the winner node. 
    Returns the result of the task execution on successful assignment, None on failure.
    """

    # Get the lan, node_id and IP of such peer in the node.peers list for which node_id equals peer_id
    winner_info = next(peer for peer in node.peers if peer[1] == winner_id)

    if not winner_info:
        return None
    
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
    
    task_exec_result = {}
    
    # Assign task to winner node and get executed task result back
    try:
        if task_assign.payload["task_type"] != "PRIVATE_TASK":
            result = await node.bus.global_request((lan, winner_id, ip), task_assign)

        elif task_assign.payload["task_type"] == "PRIVATE_TASK":
            if not ip:
                print(f"{TAG} Skipping global node...")
            else:
                result = await node.bus.local_request((lan, winner_id, ip), task_assign)

            if result.type == "task_complete":
                print(f"{TAG} Task assigned to and executed on node {winner_id}.")
                task_exec_result = result.payload
            # TODO: handle other message types, e.g. task_exec_error, or something...
    except Exception as e:
        print(f"{TAG} task assignment error: {e}")
        return None

    return task_exec_result


def record_assignment(node, peer_id: str):
    """Record that a task was assigned to a peer. 
    peer_id is the id of the peer that was assigned a task."""

    node.assigned_task_counts[peer_id] = node.assigned_task_counts.get(peer_id, 0) + 1
    print(f"{TAG} Assignments: {node.assigned_task_counts[peer_id]}")


#async def send_task_and_get_result(node, task_id: str, task_type: str, retry_attempt: int, negotiation_results: dict):
async def do_task_assignment(node, task_id: str, task_type: str, retry_attempt: int, negotiation_results: dict):
    task_id = negotiation_results["task_id"]
    task_type = negotiation_results["task_type"]
    #winner_id = negotiation_results["node_id"]
    all_bids = negotiation_results.pop("all_bids")
    assigned = False
    
    # TODO: sleep for some time here and then simulate node failure to test node failure handling

    # Go through the sorted all bids list. Attempt to assign the task to the winner node.
    # If winner node is not available anymore, attempt to assign the task to the next node in the list.
    # Continue with this logic until the task is assigned to a node, or until there are no nodes left
    # to assign the task to.
    for candidate in all_bids:
        peer_id = candidate["node_id"]

        if all_bids.index(candidate) == 0:
            print(f"{TAG} Assigning task to the winner node {peer_id}...")
        else:
            print(f"{TAG} Assigning task to node no. {all_bids.index(candidate) + 1} in the ranking list...")
                
        task_assign_time = time()   # T3: task assignment sent
        result = await send_task_to_be_executed_and_receive_result(node, peer_id, task_id, task_type)

        if result:
            record_assignment(node, peer_id)
            assigned = True
            node.task_threads_and_results[task_id]["result"] = result["result"]
            print(f"{TAG} Task assignment and execution successful! Task assigned to node {peer_id}")

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
            print(f"{TAG} Current task info: {node.task_threads_and_results[task_id]}")
            break
        else:
            if all_bids.index(candidate) == 0:
                print(f"{TAG} Failed to assign task to the winner node. -- next")
                    
            #await remove_dead_node(node, peer_id)
            # NOTE: do we need this? Should we do peer cleanup with the monitor loop?

    if not assigned:
        print(f"{TAG} Task assignment failed for all nodes.")
        await enqueue_retry(node, task_type, task_id, retry_attempt)