import asyncio
import time
from llm_decision import local_llm_decide
from robustness_privacy_scoring import get_reliability_level, get_privacy_level
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
    node.bus.on("task_result", handle_task_result)


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


async def send_task_result(node, peer_info: tuple, task_result: Message):
    return await node.bus.request((peer_info[0], peer_info[1], peer_info[2]), task_result)


def execute_task(node, task_id: str, orig_peer_id: str):
    # Simulate task execution with a sleep
    print(f"{TAG} Executing {task_id} ...")
    time.sleep(5)

    node.state["is_busy"] = False
    record_task(node, task_id)
    #node.state["tasks_completed"] = len(node.task_cache)
    node.state["tasks_completed"] += 1

    print(f"{TAG} Task {task_id} complete!")

    task_result = Message(
                    type="task_result",
                    originator_node=node.id,
                    originator_lan=node.lan,
                    payload={
                        "task_id": task_id,
                        "result": "Task execution result: successful!"
                    })

    peer_info = next((p for p in node.peers if p[1] == orig_peer_id), None)

    if peer_info is None:
        print(f"{TAG} Task result for {task_id} cannot be sent: originator not found.")
        #TODO: maybe log the incident and discard the result, since the originator is no longer reachable?
        return

    # TODO: fixt the task result sending to the originator node!
    #NOTE: Old version: response = asyncio.run(node.bus.global_request((peer_info[0], peer_info[1], peer_info[2]), task_result))
    response = asyncio.run(send_task_result(node, peer_info, task_result))

    print(f"{TAG} Task result for {task_id}: {task_result.payload['result']}")
    if response.type == "ack":
        print(f"{TAG} Task result for {task_id} acknowledged by originator.")


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

    Thread(target=execute_task, args=(node, task_id, originator_node), daemon=True).start()

    return {"type": "ack", 
            "payload": {
                "task_id": task_id
    }}


def handle_task_result(node, task_result: dict, originator_lan: str, originator_node: str) -> dict:
    task_id = task_result.get("task_id")
    #task_type = task_result.get("task_type")
    result = task_result.get("result")

    node.task_results.append({"task_id": task_id, "result": result})

    print(f"{TAG} Received task result for {task_id} from node {originator_node}")

    return {"type": "ack", 
            "payload": {
                "task_id": task_id
    }}