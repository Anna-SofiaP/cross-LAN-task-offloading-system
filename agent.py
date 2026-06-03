from dataclasses import dataclass
from messagebus import MessageBus
from node import Node


TAG = "[Agent]"

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict

def register(node: Node):
    """Register agent's message handlers to the node's MessageBus."""
    node.bus.on("task_request", handle_task_request)
    node.bus.on("task_assignment", handle_task_assignment)


# TODO: add also the messagebus as a parameter here, for sending bids
def handle_task_request(task_req: dict) -> dict:
    task_id = task_req.get("task_id")
    task_type = task_req.get("task_type")

    print(f"\n{TAG} Received task request: task id={task_id}, type={task_type}")

    return {"msg": "ack" }


def handle_task_assignment(bus: MessageBus, task_assignment: Message):
    pass