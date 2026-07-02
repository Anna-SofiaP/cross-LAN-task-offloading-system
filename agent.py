import time
from llm_decision import local_llm_decide
from robustness_privacy_scoring import get_reliability_level, get_privacy_level
from threading import Thread

TAG = "[AGENT]"


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
    originator_lan = bid_req.get("originator_lan")
    payload = bid_req.get("payload", {})

    in_data_privacy_lvl = payload["in_data_privacy_lvl"]
    out_data_privacy_lvl = payload["out_data_privacy_lvl"]
    task_priority = payload["task_priority"]

    print(f"\n{TAG} Received bid request: " +
          f"     task id={task_id}" +
          f"     type={task_type}\n")
    
    node_state = node.state

    # TODO: Give this info to LLM!!!
    reliability_level = get_reliability_level(node, node_state, originator_lan)
    privacy_level = get_privacy_level(node_state)
    
    print(f"[{TAG}] Evaluating {task_id}  score={node_state['score']:.4f} " \
          f"risk={node_state['risk']}  busy={node_state.get('is_busy', False)} " \
          f"reliability_level={reliability_level}  privacy_level={privacy_level}")
    
    llm_decision = local_llm_decide(node_state, node.id, node.llm_tok, node.llm_mdl, task_type, 
                                    reliability_level, privacy_level, 
                                    in_data_privacy_lvl, out_data_privacy_lvl, task_priority)

    print(f"{TAG} Decision: {llm_decision['decision']}  reason: {llm_decision['reason']}")

    if llm_decision["decision"] != "ACCEPT":
        print(f"{TAG} Not bidding -- REJECT")
        return {"type": "bid", "payload": {"task_id": task_id, "decision": "REJECT"}}

    print(f"{TAG} Sending bid with score={node_state['score']:.4f}")

    bid = {"type": "bid",
           "payload": {
               "task_id": task_id,
               "node_id": node.id,
               "score": node_state["score"],
               "risk": node_state["risk"],
               "reason": llm_decision["reason"], 
               "decision": "ACCEPT"
            }
    }

    return bid


def record_task(node, task_id: str):
    node.task_cache.append({"success": 1})


def execute_task(node, task_id: str):
    print(f"{TAG} Executing {task_id} ...")
    time.sleep(5)

    node.state["is_busy"] = False
    record_task(node, task_id)
    #node.state["tasks_completed"] = len(node.task_cache)
    node.state["tasks_completed"] += 1
    # TODO:
    # if some task privacy constraint:
    #   node.state["high_privacy_tasks_completed"] += 1

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
        node.state["tasks_assigned"] += 1 

    Thread(target=execute_task, args=(node, task_id,), daemon=True).start()

    return {"type": "ack", 
            "payload": {
                "task_id": task_id
    }}