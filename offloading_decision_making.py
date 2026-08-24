# Node and task matching ------------------------------------------------------------
TASK_DATA_PRIVACY_REQUIREMENTS = {  # Match task in data privacy and out data privacy levels with node privacy level
    "PUBLIC":       "low",
    "INTERNAL":     "moderate",
    "CONFIDENTIAL": "high",
    "RESTRICTED":   "high",
}


TASK_PRIORITY_REQUIREMENTS = {  # Match task priority level with node reliability level
    "LOW":          "low",
    "MEDIUM":       "moderate",
    "HIGH":         "high",
}

# Scalar values to reliability and privacy levels -----------------------------------
LEVELS_TO_SCALARS = {
    "high": 3,
    "moderate": 2,
    "low": 1
}

# Score calculation constants -------------------------------------------------------
LOAD_PENALTY = 0.05        # score penalty per task above average
NEW_NODE_BONUS = 0.03      # bonus for nodes with 0 assignments

NORM_DIVISOR = 2
DISCARD_LIMIT = -2
ABSOLUTE_DIFFERENCE_LIMIT = -1
LOW_W_SUM_PENALTY_VAL = 1.2
HIGH_W_SUM_PENALTY_VAL = 1.5
ZERO_SAFEGUARD = 0.1    # To prevent weighted sum from being 0 (and division by that 0 later in the code)
PRIVACY_WEIGHT = 0.5
RELIABILITY_WEIGHT = 0.5

PR_SCORE_WEIGHT = 0.6       # NOTE: PR = privacy-reliability
ADJ_SCORE_WEIGHT = 0.4      # NOTE: ADJ = adjusted

# General ---------------------------------------------------------------------------
TAG = "[OFFLOAD]"


def load_balanced_score(node, bid: dict, all_bidders: list = None) -> float:
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
    peer_id = bid.get("node_id")
    raw = bid.get("score", 0.0)

    #print(f"{TAG} Calculating load balanced score for peer {peer_id}, score={raw}...")

    task_assign_counts = dict(node.assigned_task_counts)

    # Build a map of the task assignments counts for all live peers, including live nodes with 0 assignments
    # Get the node_id value from the peer info tuple, for each peer in the peer list
    live_peers = [peer[1] for peer in node.peers]
    if all_bidders:
        live_peers = list(set(live_peers) | set(all_bidders))

    #print(f"{TAG} All live peers: {live_peers}")

    # Get the task assignment counts of all peers.
    all_task_assign_counts = {k: task_assign_counts.get(k, 0) for k in live_peers}
    if not all_task_assign_counts:
        return round(raw + NEW_NODE_BONUS, 4)

    # Get the task assignment counts of the currently examined peer.
    peer_task_assign_counts = all_task_assign_counts.get(peer_id, 0)
    #print(f"{TAG} Task assign count of peer {peer_id}: {peer_task_assign_counts}")

    avg = sum(all_task_assign_counts.values()) / max(len(all_task_assign_counts), 1)

    penalty = LOAD_PENALTY * max(0.0, peer_task_assign_counts - avg)
    bonus   = NEW_NODE_BONUS if peer_task_assign_counts == 0 else 0.0
    adj     = round(raw - penalty + bonus, 4)

    print(f"{TAG} Calculation results for {peer_id}:" \
          f"    penalty = {penalty}" \
          f"    bonus = {bonus}" \
          f"    adjusted score = {adj}")
    
    final_score = max(0.0, min(1.0, adj))
    #bid["adj_score"] = final_score

    return final_score



# TODO: Remove, and just use scalars in the whole level assignment process!
def level_to_scalar(level):
    scalar = LEVELS_TO_SCALARS[level]
    #print(f"{TAG} Level: {level}, Scalar: {scalar}, Scalar type: {type(scalar)}")
    return scalar



def reliability_and_privacy_assessment(bid: dict, in_data_privacy_lvl, out_data_privacy_lvl, task_priority, my_lan):
    # Peer node privacy and reliability levels
    peer_id = bid.get("node_id")
    peer_lan = bid.get("peer_lan")

    node_reliability_lvl = level_to_scalar(bid.get("reliability_lvl", "moderate"))    # In reliability it is okay to give the node a chance
    node_privacy_lvl = level_to_scalar(bid.get("privacy_lvl", "low"))                 # Do not trust nodes by default regarding privacy

    # Required privacy and reliability levels for the task
    in_privacy_requirement = level_to_scalar(TASK_DATA_PRIVACY_REQUIREMENTS.get(in_data_privacy_lvl, "CONFIDENTIAL"))
    #out_privacy_requirement = level_to_scalar(TASK_DATA_PRIVACY_REQUIREMENTS.get(out_data_privacy_lvl, "PUBLIC"))
    reliability_requirement = level_to_scalar(TASK_PRIORITY_REQUIREMENTS.get(task_priority, "MEDIUM"))

    if (in_data_privacy_lvl in ["CONFIDENTIAL", "RESTRICTED"]) and (peer_lan != my_lan):
        print(f"{TAG} Node does not meet task's strict input data privacy requirement -- discarding node")
        bid["pr_score"] = DISCARD_LIMIT
        return DISCARD_LIMIT

    # Calculate difference between node privacy/reliability level and task requirements
    p_delta = node_privacy_lvl - in_privacy_requirement if node_privacy_lvl >= in_privacy_requirement else DISCARD_LIMIT
    r_delta = node_reliability_lvl - reliability_requirement if node_reliability_lvl >= reliability_requirement else DISCARD_LIMIT

    # Discard nodes with too big a gap between task requirement and node privacy or reliability level
    if p_delta == DISCARD_LIMIT or r_delta == DISCARD_LIMIT:
        print(f"{TAG} Node {peer_id} has too low p_delta={p_delta} or r_delta={r_delta} value -- discarding node")
        bid["pr_score"] = DISCARD_LIMIT
        return DISCARD_LIMIT
    
    # If p_delta or r_delta value is -1, don't discard, but give the node a higher w_sum value than in the actual w_sum calculation
    if p_delta == ABSOLUTE_DIFFERENCE_LIMIT and r_delta == ABSOLUTE_DIFFERENCE_LIMIT:
        print(f"{TAG} p_delta and r_delta are both slightly too low -- w_sum={HIGH_W_SUM_PENALTY_VAL}")
        return HIGH_W_SUM_PENALTY_VAL
    elif p_delta == ABSOLUTE_DIFFERENCE_LIMIT ^ r_delta == ABSOLUTE_DIFFERENCE_LIMIT:
        print(f"{TAG} Either p_delta or r_delta has slightly too low value -- w_sum={LOW_W_SUM_PENALTY_VAL}")
        return LOW_W_SUM_PENALTY_VAL
    
    # Normalize privacy delta and reliability delta values to [0, 1]:
    p_norm = p_delta / NORM_DIVISOR
    r_norm = r_delta / NORM_DIVISOR

    # Calculate weighted sum
    w_sum = (PRIVACY_WEIGHT * p_norm + RELIABILITY_WEIGHT * r_norm) + ZERO_SAFEGUARD

    print(f"{TAG} {peer_id}: p_delta={p_delta}, r_delta={r_delta}, p_norm={p_norm}, r_norm={r_norm}, w_sum={w_sum}")

    #bid["pr_score"] = w_sum

    return w_sum


def make_final_offloading_decision(bid: dict, all_valid_bids: list):
    peer_id = bid.get("node_id")

    # Calculate adjusted score and privacy-reliability score divisors for normalization
    adj_score_divisor = sum([b["adj_score"] for b in all_valid_bids])
    pr_score_divisor = sum([1 / b["pr_score"] for b in all_valid_bids])

    print(f"{TAG} a_score_divisor={adj_score_divisor}, pr_score_divisor={pr_score_divisor}")

    # Normalize values and calculate weighted product
    adj_score_norm = bid["adj_score"] / adj_score_divisor
    pr_score_norm = (1 / bid["pr_score"]) / pr_score_divisor

    w_product = (adj_score_norm ** ADJ_SCORE_WEIGHT) * (pr_score_norm ** PR_SCORE_WEIGHT)

    print(f"{TAG} {peer_id}: a_score_norm={adj_score_norm}, pr_score_norm={pr_score_norm}, w_product={w_product}")

    bid["w_product"] = w_product

    return w_product