"""
monitor.py — Heartbeat and peer management

Plain functions that run as background loops.
start() is called once at startup and runs forever.
"""

import asyncio
import psutil
import numpy as np
from node import Node
from lstm_scoring import compute_rel, compute_rep, compute_score, risk_level, predict_horizon

TAG = "[MONITOR]"
HEARTBEAT_INTERVAL = 20
DISK_PATH = "/"
SAMPLE_INTERVAL  = 1.0


async def heartbeat_loop(node: Node):
    """Broadcast this node's presence and status to the whole cluster."""

    print(f"{TAG} Sending heartbeat signal to peers every {HEARTBEAT_INTERVAL} seconds.\n")

    while True:
        try:
            await node.bus.publish_heartbeat(lan=node.lan)
            print(
                f"{TAG} Heartbeat sent. "
                f"Known peers: {list(node.bus.peers)}\n"
            )
        except Exception as e:
            print(f"{TAG} Heartbeat error: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def metric_loop(node: Node):
    """
    Continuously sample resource metrics, and compute predictions and suitability scores. 
    Update state of the node and print summary.
    """

    print(f"{TAG} {node.id} Metric loop started")

    while True:
        cpu  = psutil.cpu_percent(interval=0.1)
        mem  = psutil.virtual_memory().percent
        disk = psutil.disk_usage(DISK_PATH).percent
        sample = np.array([cpu/100, mem/100, disk/100], np.float32)

        node.resource_history.append(sample)
        n = len(node.resource_history)
        ready = n >= node.window_len
        preds = predict_horizon(node.resource_history, node.window_len, node.lstm_model)
        
        avg = preds.mean(axis=0)

        rep = compute_rep(node.task_cache)
        rel = compute_rel(node.task_cache)
        score = compute_score(preds.tolist(), rep, rel)
        risk  = risk_level(score)

        node.state.update(score=round(score,4), risk=risk,
            reputation=round(rep,4), reliability=round(rel,4),
            cpu=round(cpu/100,4), mem=round(mem/100,4), disk=round(disk/100,4),
            cpu_pred=round(float(avg[0]),4), mem_pred=round(float(avg[1]),4),
            disk_pred=round(float(avg[2]),4), lstm_ready=ready,
            horizon=preds.tolist(), is_busy=node.state["is_busy"],
            tasks_completed=len(node.task_cache))
            
        st = "READY" if ready else f"warming {n}/{node.window_len}"

        print(f"{'-'*20}")
        print(f"{TAG} Resource metrics:" \
            f" cpu={cpu:.1f}%" \
            f" mem={mem:.1f}%" \
            f" pred_cpu={avg[0]*100:.1f}%" \
            f" score={score:.4f}" \
            f" risk={risk} [{st}]")
        print(f"{'-'*20}")
        
        await asyncio.sleep(SAMPLE_INTERVAL)