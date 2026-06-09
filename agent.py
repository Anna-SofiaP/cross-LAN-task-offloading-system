import asyncio
from dataclasses import dataclass


TAG = "[Agent]"

@dataclass
class Message:
    type: str
    originator_node: str
    originator_lan: str
    payload: dict


# ==================================================================================
# Local messaging (ZeroMQ) server
# ==================================================================================

async def start(node):
    while True:
        msg = await node.bus.get_local_message()
        if msg == None:
            print(f"{TAG} No bid received from local peer.")
            continue
        task_id = msg.payload.get("task_id")
        task_type = msg.payload.get("task_type")

        if task_type == "task_request":
            print(f"\n{TAG} Received task request: task id={task_id}, type={task_type}")
            await node.bus.local_request({"msg": "ack"})


# ===================================================================================
# NATS message handlers
# ===================================================================================


def register(node):
    """Register agent's message handlers to the node's MessageBus."""
    node.bus.on("task_request", handle_task_request)
    node.bus.on("task_assignment", handle_task_assignment)


# TODO: add also the messagebus as a parameter here, for sending bids
def handle_task_request(task_req: dict) -> dict:
    task_id = task_req.get("task_id")
    task_type = task_req.get("task_type")

    print(f"\n{TAG} Received task request: " +
          "     task id={task_id}" +
          "     type={task_type}\n")

    # TODO: send back also originator_node id and originator_lan in the reply
    return {"msg": "ack"}


def handle_task_assignment(task_assignment: Message):
    pass