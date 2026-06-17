from dataclasses import dataclass
from time import time


TAG = "[ASSIGN]"
MAX_RETRIES = 3           # max retry attempts for a deferred task

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


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
    
    # Assign task to winner node and get executed task result back
    try:
        if task_assign.payload["task_type"] != "PRIVATE_TASK":
            ack = await node.bus.global_request((lan, winner_id, ip), task_assign)

        elif task_assign.payload["task_type"] == "PRIVATE_TASK":
            if not ip:
                print(f"{TAG} Skipping global node...")
            else:
                ack = await node.bus.local_request((lan, winner_id, ip), task_assign)

            if ack.type == "ack":
                print(f"{TAG} Task assigned to and executed on node {winner_id}.")
    except Exception as e:
        print(f"{TAG} task assignment error: {e}")
        return False

    return True


def record_assignment(node, peer_id: str):
    """Record that a task was assigned to a peer. 
    peer_id is the id of the peer that was assigned a task."""

    node.assigned_task_counts[peer_id] = node.assigned_task_counts.get(peer_id, 0) + 1
    print(f"{TAG} Assignments: {node.assigned_task_counts[peer_id]}")