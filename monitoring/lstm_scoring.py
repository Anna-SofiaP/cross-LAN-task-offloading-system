import numpy as np

HORIZON_H = 5
TAG = "[SCORING]"


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
#def compute_rep(node_task_cache):
def compute_rep(tasks_assigned, tasks_completed):
    """Compute simple reputation: ratio of successful tasks, with smoothing for new nodes."""
    if tasks_assigned == 0:
        return 0.5
    return max(0.0, min(1.0, (tasks_completed + 3) / (tasks_assigned + 6)))


#def compute_rel(node_task_cache):
def compute_rel(tasks_assigned, tasks_completed):
    """Compute reliability: weighted recent success rate, more forgiving to new nodes."""
    if tasks_assigned < 3:
        return 0.6
    on = tasks_completed / tasks_assigned
    return max(0.0, min(1.0, 0.7*on + 0.3))


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