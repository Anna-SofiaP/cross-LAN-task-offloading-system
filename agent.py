import time
from llm_decision import local_llm_decide
from threading import Thread

TAG = "[Agent]"


def register(node):
    """Register agent's message handlers to the node's MessageBus."""
    node.bus.on("task_request", handle_task_request)
    node.bus.on("bid_request", handle_bid_request)
    node.bus.on("task_assignment", handle_task_assignment)


def handle_task_request(node, task_req: dict) -> dict:
    task_id = task_req.get("task_id")
    task_type = task_req.get("task_type")

    print(f"\n{TAG} Received task request: " +
          f"     task id={task_id}" +
          f"     type={task_type}\n")

    return {"type": "ack", "payload": {}}


def handle_bid_request(node, bid_req: dict) -> dict:
    task_id = bid_req.get("task_id")
    task_type = bid_req.get("task_type")

    print(f"\n{TAG} Received bid request: " +
          f"     task id={task_id}" +
          f"     type={task_type}\n")
    
    node_resource_state = node.state
    
    print(f"[{TAG}] Evaluating {task_id}  score={node_resource_state['score']:.4f} " \
          f"risk={node_resource_state['risk']}  busy={node_resource_state.get('is_busy', False)}")
    
#    llm_decision = local_llm_decide(node_resource_state, node.id, node.llm_tok, node.llm_mdl, task_type)
#
#    print(f"{TAG} Decision: {llm_decision['decision']}  reason: {llm_decision['reason']}")
#
#    if llm_decision["decision"] != "ACCEPT":
#        print(f"{TAG} Not bidding -- REJECT")
#        return {"type": "bid", "payload": {"task_id": task_id, "decision": "REJECT"}}

    print(f"{TAG} Sending bid with score={node_resource_state['score']:.4f}")

    bid = {"type": "bid",
           "payload": {
               "task_id": task_id,
               "node_id": node.id,
               "score": node_resource_state["score"],
               "risk": node_resource_state["risk"],
#               "reason": llm_decision["reason"], 
               "decision": "ACCEPT"
            }
    }

    return bid


def record_task(node, task_id: str):
    # TODO: change the idea of this task cache, it is not working very well now...
    node.task_cache.append({"success": 1})


def execute_task(node, task_id: str):
    print(f"{TAG} Executing {task_id} ...")
    time.sleep(5)

    node.state["is_busy"] = False
    record_task(node, task_id)
    node.state["tasks_completed"] = len(node.task_cache)

    print(f"{TAG} Task {task_id} complete")


def handle_task_assignment(node, task_assignment: dict):
    task_id = task_assignment.get("task_id")
    task_type = task_assignment.get("task_type")
    winner_id = task_assignment.get("winner_id")

    if winner_id == node.id:
        print(f"\n{TAG} Received task assignment:\n" +
              f"     task id={task_id}\n" +
              f"     type={task_type}\n")
        
        node.state["is_busy"] = True

    Thread(target=execute_task, args=(node, task_id,), daemon=True).start()

    return {"type": "ack", 
            "payload": {
                "task_id": task_id
    }}