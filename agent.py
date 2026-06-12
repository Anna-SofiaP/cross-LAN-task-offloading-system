import asyncio
from dataclasses import dataclass
import json
from llm_decision import local_llm_decide


TAG = "[Agent]"

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
#               "decision": "ACCEPT"
            }
    }

    return bid


def handle_task_assignment(node, task_assignment: Message):
    pass