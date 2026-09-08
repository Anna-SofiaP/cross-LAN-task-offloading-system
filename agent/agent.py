import asyncio
import time
from llm_decision import local_llm_decide
#from robustness_privacy_scoring import get_reliability_level, get_privacy_level
from threading import Thread
from dataclasses import dataclass

TAG = "[AGENT]"

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict = None


def register(node):
    """Register agent's message handlers to the node's MessageBus."""
    node.bus.on("task_request", handle_task_request)
    node.bus.on("bid_request", handle_bid_request)
    node.bus.on("task_assignment", handle_task_assignment)
    node.bus.on("task_result_request", handle_task_result_request)


def handle_task_request(node, task_req: dict, originator_lan: str, originator_node: str) -> dict:
    task_id = task_req.get("task_id")
    task_type = task_req.get("task_type")

    print(f"\n{TAG} Received task request: " +
          f"     task id={task_id}" +
          f"     type={task_type}\n")

    return {"type": "ack", "payload": {}}


def handle_bid_request(node, bid_req: dict, originator_lan: str, originator_node: str) -> dict:
    task_id = bid_req.get("task_id")
    task_type = bid_req.get("task_type")

    in_data_privacy_lvl = bid_req.get("in_data_privacy_lvl")
    out_data_privacy_lvl = bid_req.get("out_data_privacy_lvl")
    task_priority = bid_req.get("task_priority")

    print(f"\n{TAG} Received bid request: " +
          f"     task id={task_id}" +
          f"     type={task_type}\n")
    
    node_state = node.state
    my_lan = node.lan

    # NOTE: for eval tests! -------------------------------------------------------------
    reliability_level = node_state["reliability_lvl"]
    privacy_level = node_state["privacy_lvl"]
    # ------------------------------------------------------------------------------------
    
    print(f"[{TAG}] Evaluating {task_id}  score={node_state['score']:.4f} " \
          f"risk={node_state['risk']}  busy={node_state.get('is_busy', False)} " \
          f"reliability_level={reliability_level}  privacy_level={privacy_level}")
    
    llm_decision = local_llm_decide(node_state, node.id, node.llm_tok, node.llm_mdl, task_type)

    if llm_decision["decision"] != "ACCEPT":
        print(f"{TAG} Not bidding -- REJECT")
        return {"type": "bid", "payload": {"task_id": task_id, "decision": "REJECT"}}
    
    print(f"{TAG} Decision: {llm_decision['decision']}\n  reason: {llm_decision['reason']}")

    bid = {"type": "bid",
           "payload": {
               "task_id": task_id,
               "node_id": node.id,
               "peer_lan": my_lan,
               "score": node_state["score"],
               "risk": node_state["risk"],
               "reliability_lvl": reliability_level,
               "privacy_lvl": privacy_level,
               "reason": llm_decision["reason"], 
               "decision": "ACCEPT"
            }
    }

    return bid


def mark_task_completed(node, task_id: str):
    """Mark a task as completed and update the node's state accordingly."""
    node.state["is_busy"] = False
    node.state["tasks_completed"] += 1

    #node.task_cache.append({"success": 1})

    print(f"{TAG} Task {task_id} complete!")


def execute_task(node, task_id: str, task_type: str, originator_node: str):
    '''Execute the task and add the result to the completed tasks list. Task execution is simulated with a sleep.'''

    print(f"{TAG} Executing {task_id} ...")
    time.sleep(2)  # Simulate task execution time

    mark_task_completed(node, task_id)

    node.completed_tasks_results.append({"originator_peer": originator_node,
                                         "task_id": task_id, 
                                         "task_type": task_type, 
                                         "result": "Task execution result: successful!"})


def handle_task_assignment(node, task_assignment: dict, originator_lan: str, originator_node: str) -> dict:
    task_id = task_assignment.get("task_id")
    task_type = task_assignment.get("task_type")
    winner_id = task_assignment.get("winner_id")

    if winner_id == node.id:
        print(f"\n{TAG} Received task assignment:\n" +
              f"     task id={task_id}\n" +
              f"     type={task_type}\n")
        
        node.state["is_busy"] = True
        node.state["tasks_assigned"] += 1 

    Thread(target=execute_task, args=(node, task_id, task_type, originator_node), daemon=True).start()

    return {"type": "ack", 
            "payload": {
                "task_id": task_id
    }}


def handle_task_result_request(node, task_result_req: dict, originator_lan: str, originator_node: str) -> dict:
    task_id = task_result_req.get("task_id")
    task_type = task_result_req.get("task_type")

    print(f"\n{TAG} Received task result request:\n" +
          f"     task id={task_id}\n" +
          f"     type={task_type}\n")
    
    # Search for the task result in the completed tasks list
    task_result = next((result for result in node.completed_tasks_results 
                        if (result["task_id"] == task_id and result["originator_peer"] == originator_node)), None)
    
    if not task_result:
        print(f"{TAG} Task result for {task_id} not found.")
        return {"type": "no_result", "payload": {}}

    print(f"{TAG} Found result for task {task_id}: {task_result['result']}. Sending result to task originator node {originator_node}.")

    # The task and its result was found: remove the result from the completed tasks list...
    indx = node.completed_tasks_results.index(task_result)
    node.completed_tasks_results.pop(indx)

    # ... and send the result to the task originator
    return {"type": "result", "payload": task_result["result"]}