import csv
from pathlib import Path
import time

OFFLOADING_DECISION_RESULT_LOG = Path("./evaluation_tests/successful_offloading_results.csv")
FAILED_OFFLOADING_RESULT_LOG = Path("./evaluation_tests/failed_offloading_results.csv")



def save_offloading_decision_result(task_no, task_id, task_type, in_data_privacy_lvl, out_data_privacy_lvl, task_priority,
                                    winner_id, winner_lan, winner_adj_score, winner_pr_score, winner_final_score,
                                    winner_reliability_lvl, winner_privacy_lvl, winner_decision,
                                    all_bids, lat_total_ms, retry_attempt, expected_result):

    second_p_id = all_bids[1]["node_id"] if len(all_bids) > 1 else "NA"
    second_p_lan = all_bids[1]["peer_lan"] if len(all_bids) > 1 else "NA"
    second_p_adj_score = all_bids[1]["adj_score"] if len(all_bids) > 1 else "NA"
    second_p_pr_score = all_bids[1]["pr_score"] if len(all_bids) > 1 else "NA"
    second_p_final_score = all_bids[1]["w_product"] if len(all_bids) > 1 else "NA"

    third_p_id = all_bids[2]["node_id"] if len(all_bids) > 2 else "NA"
    third_p_lan = all_bids[2]["peer_lan"] if len(all_bids) > 2 else "NA"
    third_p_adj_score = all_bids[2]["adj_score"] if len(all_bids) > 2 else "NA"
    third_p_pr_score = all_bids[2]["pr_score"] if len(all_bids) > 2 else "NA"
    third_p_final_score = all_bids[2]["w_product"] if len(all_bids) > 2 else "NA"

    header = not OFFLOADING_DECISION_RESULT_LOG.exists()
            
    with OFFLOADING_DECISION_RESULT_LOG.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "t_no", "t_id", "t_type", "in_data_privacy_lvl", "out_data_privacy_lvl", "t_priority",
            "win_id", "expected_res", "win_lan", "win_adj_score", "win_pr_score", "win_final_score",
            "win_reliability_lvl", "win_privacy_lvl", "win_decision",
            "sec_id", "sec_lan", "sec_adj_score", "sec_pr_score", "sec_final_score",
            "thi_id", "thi_lan", "thi_adj_score", "thi_pr_score", "thi_final_score",
            "lat_total_ms", "retry_attempt", "timestamp"]
        )
        
        if header: 
            w.writeheader()

        w.writerow(dict(
            t_no=task_no,
            t_id=task_id,
            t_type=task_type,
            in_data_privacy_lvl=in_data_privacy_lvl,
            out_data_privacy_lvl=out_data_privacy_lvl,
            t_priority=task_priority,
            win_id=winner_id,
            expected_res=expected_result,
            win_lan=winner_lan,
            win_adj_score=winner_adj_score,
            win_pr_score=winner_pr_score,
            win_final_score=winner_final_score,
            win_reliability_lvl=winner_reliability_lvl,
            win_privacy_lvl=winner_privacy_lvl,
            win_decision=winner_decision,
            sec_id=second_p_id,
            sec_lan=second_p_lan,
            sec_adj_score=second_p_adj_score,
            sec_pr_score=second_p_pr_score,
            sec_final_score=second_p_final_score,
            thi_id=third_p_id,
            thi_lan=third_p_lan,
            thi_adj_score=third_p_adj_score,
            thi_pr_score=third_p_pr_score,
            thi_final_score=third_p_final_score,
            lat_total_ms=round(lat_total_ms,1),
            retry_attempt=retry_attempt,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        ))
        

def save_failed_offloading_result(task_no, task_id, task_type, 
                                  in_data_privacy_lvl, out_data_privacy_lvl, task_priority,
                                  failure_reason):
    
    header = not FAILED_OFFLOADING_RESULT_LOG.exists()

    with FAILED_OFFLOADING_RESULT_LOG.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["t_no", "task_id", "task_type", 
                                          "in_data_privacy_lvl", "out_data_privacy_lvl", "task_priority",
                                          "failure_reason"])
        
        if header: 
            w.writeheader()

        w.writerow(dict(
            t_no=task_no, task_id=task_id, task_type=task_type,
            in_data_privacy_lvl=in_data_privacy_lvl, out_data_privacy_lvl=out_data_privacy_lvl,
            task_priority=task_priority, failure_reason=failure_reason
        ))