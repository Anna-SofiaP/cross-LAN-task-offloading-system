"""
# Reliability and privacy scoring
This module is for defining the different robustness/reliability, trustworthiness, 
accessibility and privacy scoring systems and levels that are used in the task 
allocation process. Each node it put on some level in each three different categories: 
- node reliability
- node privacy
- network trustworthiness and accessibility

## Categorizing the nodes

There are three levels in all categories, in which nodes can be put:
- high
- moderate
- low

Nodes can be on different levels in different categories.

The score that determines which level the node will be put on, is calculated by 
multiplying each score in that category with each other. This way we will always get
a score between 0 and 1. For example, if the node has had 1 node failure within the
14 day period, and it has a task assignment success rate of 0.6, the score will be:

`1.0 x 0.6 = 0.6`.

The predefined `RELIABILITY_LEVELS` shows that a node with a score 0.6
should be put in the `moderate` category, since the score falls between the `min` and
the `max` values of that level.

## About forming of the levels
The maximum and minimum values of the levels have been calculated and formulated roughly,
by...
"""

TAG = "[LEVELS]"

# Reliability ----------------------------------------------------------------------
NODE_FAILURE_SCORE = [(0, 1, 1.0),          # (min_value, max_value, score):
                      (2, 3, 0.8),          # min_value: minimum no. of node failures in a 7 day period 
                      (4, 4, 0.6)]          # max_value: maximum no. of node failures in a 7 day period

NODE_ACCESS_SCORE = (1.0, 0.8) # 1.0 = node is in the same LAN as the task originator, 0.8 = node is in a different LAN

RELIABILITY_LEVELS = {
                        "high": {
                            "max": 1.00, 
                            "min": 0.62
                            }, 
                        "moderate": {
                            "max": 0.61,
                            "min": 0.37
                        },
                        "low": {
                            "max": 0.36,
                            "min": 0.00
                        }
                        
                    }

# Privacy --------------------------------------------------------------------------
#PRIVACY_TASK_EXEC_SCORE = [(0.9, 1.0, 1.0),     # (min_value, max_value, score)
#                           (0.7, 0.9, 0.8), 
#                           (0.5, 0.7, 0.6), 
#                           (0.0, 0.5, 0.4)]

DEVICE_USER_TYPE_SCORE = [("business", 1.0), 
                   ("personal", 0.8), 
                   ("public", 0.4)]

NETWORK_TRUST_SCORE = [("usr-auth-network", 1.0), 
                       ("private-network", 0.8), 
                       ("public-network", 0.4)]

PRIVACY_LEVELS = {
                    "high": {
                        "max": 1.00, 
                        "min": 0.64
                        }, 
                    "moderate": {
                        "max": 0.63,
                        "min": 0.48
                    },
                    "low": {
                        "max": 0.47,
                        "min": 0.00
                    }  
                }


def get_network_trustworthiness_score(network_type, network_trust_score = 0.4):
    for n_type, score in NETWORK_TRUST_SCORE:
        if n_type == network_type:
            network_trust_score = score
    
    print(f"{TAG} Network trustworthiness score: {network_trust_score}")
    return network_trust_score


def get_device_user_type_score(device_user_type, usr_type_score = 0.4):
    for usr_type, score in DEVICE_USER_TYPE_SCORE:
        if usr_type == device_user_type:
            usr_type_score = score

    print(f"{TAG} Device user type score: {usr_type_score}")
    return usr_type_score


def get_node_failure_score(node_failures, node_failure_score = 1.0):
    for min_value, max_value, score in NODE_FAILURE_SCORE:
        if min_value <= node_failures <= max_value:
            node_failure_score = score

    print(f"{TAG} Node failure score: {node_failure_score}")
    return node_failure_score


# TODO: test this function and make sure it works as intended!
def get_reliability_level(node, node_state: dict, lan: str) -> str:
    # Required information for scoring
    assigned_tasks = node_state.get("tasks_assigned", 0)
    completed_tasks = node_state.get("tasks_completed", 0)
    node_failures = node_state.get("node_failures", 0)
    my_lan = node.lan

    # Score initialization and calculation
    # Task assignment success score: smoothing for new nodes (0.5), so no multiplication with zero
    task_assignment_success_score = completed_tasks / assigned_tasks if assigned_tasks > 0 else 0.5
    node_failure_score = get_node_failure_score(node_failures)
    node_access_score = NODE_ACCESS_SCORE[0] if my_lan == lan else NODE_ACCESS_SCORE[1]

    combined_score = task_assignment_success_score * node_failure_score * node_access_score

    # Determine the reliability level based on the combined score
    reliability_level = "low"

    for level, bounds in RELIABILITY_LEVELS.items():
        if bounds["min"] <= combined_score <= bounds["max"]:
            reliability_level = level
            break

    print(f"{TAG} Reliability level: {reliability_level}")
 
    return reliability_level


# TODO: test this function and make sure it works as intended!
def get_privacy_level(node_state: dict) -> str:
    usr_type_score = node_state.get("usr_type_score")
    network_trust_score = node_state.get("network_trust_score")

    combined_score = usr_type_score * network_trust_score

    # Determine the privacy level based on the combined score
    privacy_level = "low"

    for level, bounds in PRIVACY_LEVELS.items():
        if bounds["min"] <= combined_score <= bounds["max"]:
            privacy_level = level
            break

    print(f"{TAG} Privacy level: {privacy_level}")

    return privacy_level