import numpy as np

HORIZON_H = 5
LOAD_PENALTY = 0.05        # score penalty per task above average
NEW_NODE_BONUS = 0.03      # bonus for nodes with 0 assignments


# ---- LSTM prediction -----------------------------------------------------
def predict_horizon(history, window_len, lstm_model):
    """Predict short-term resource usage horizon using LSTM model."""

    if len(history) < window_len:
        return np.full((HORIZON_H, 3), 0.5, np.float32)
    w = np.array(list(history)[-window_len:], np.float32)[np.newaxis]
    preds = []
    for _ in range(HORIZON_H):
        p = np.clip(lstm_model.predict(w, verbose=0)[0], 0, 1)
        preds.append(p)
        w = np.roll(w, -1, axis=1); w[0,-1,:] = p
    return np.array(preds, np.float32)


# ---- Scoring -------------------------------------------------------------
def compute_rep(node_task_cache):
    """Compute simple reputation: ratio of successful tasks, with smoothing for new nodes."""
    c = list(node_task_cache)
    if not c: 
        return 0.5
    s = sum(1 for t in c if t.get("success")==1)
    return max(0.0, min(1.0, (s+3)/(len(c)+6)))


def compute_rel(node_task_cache):
    """Compute reliability: weighted recent success rate, more forgiving to new nodes."""
    c = list(node_task_cache)
    if len(c) < 3: 
        return 0.6
    on = sum(1 for t in c if t.get("success")==1)/len(c)
    return max(0.0, min(1.0, 0.7*on+0.3))


def compute_score(horizon, rep, rel):
    """Compute composite suitability score based on predicted resource usage, reputation, and reliability."""
    s = [0.35*(1-h[0])+0.20*(1-h[1])+0.15*(1-h[2])+0.20*rep+0.10*rel
         for h in horizon[:HORIZON_H]]
    return float(sum(s)/len(s)) if s else 0.5


def risk_level(score):
    """Determine the risk level of the node based on the composite score."""
    if score >= 0.70: return "LOW"
    if score >= 0.50: return "MEDIUM"
    if score >= 0.30: return "HIGH"
    return "CRITICAL"


'''
def load_balanced_score(node, peer_id, bid: dict, all_keys: list = None) -> float:
    """
    Adjusted score = raw - penalty + bonus

    The critical fix: average is computed over ALL live nodes (including
    those with zero assignments), not just those already in _assign_counts.
    Without this, a node that always wins has avg == my_count == penalty 0.

    Example with 2 nodes, Linux has 6 tasks, Windows has 0:
      counts = {linux: 6, windows: 0}  (windows explicitly included)
      avg    = (6+0)/2 = 3.0
      linux  penalty = 0.05*(6-3) = 0.15  -> adj = 0.71-0.15 = 0.56
      windows bonus  = 0.03              -> adj = 0.60+0.03 = 0.63
      Windows wins this round.
    """
    #key = node.get("_key", node.get("ip", "?"))
    raw = bid[f"{peer_id}"].get("score", 0.0)

    #with _assign_lock: 
    counts = dict(_assign_counts)

    # Build full count map including live nodes with 0 assignments
    #with _nodes_lock: 
    #    live_keys = list(_live_nodes.keys())
    if all_keys: 
        live_keys = list(set(live_keys) | set(all_keys))

    full_counts = {k: counts.get(k, 0) for k in live_keys}
    if not full_counts:
        return round(raw + NEW_NODE_BONUS, 4)

    my  = full_counts.get(key, 0)
    avg = sum(full_counts.values()) / max(len(full_counts), 1)

    penalty = LOAD_PENALTY * max(0.0, my - avg)
    bonus   = NEW_NODE_BONUS if my == 0 else 0.0
    adj     = round(raw - penalty + bonus, 4)

    return max(0.0, min(1.0, adj))
'''