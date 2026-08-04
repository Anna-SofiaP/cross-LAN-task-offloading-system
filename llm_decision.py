import json
import re
import traceback
import torch

ACCEPT_SCORE_MIN = 0.50
TAG = "[LLM]"

TASK_PROFILES = {
    "CLASSIFICATION": ("moderate CPU", "low memory",      "ML classification"),
    "CV_INFERENCE":   ("high CPU",     "moderate memory", "computer vision"),
    "TIMESERIES":     ("moderate CPU", "moderate memory", "time-series LSTM"),
    "GENERIC":        ("moderate CPU", "moderate memory", "general compute"),
}

TASK_DATA_PRIVACY_REQUIREMENTS = {
    "PUBLIC":       ("low privacy level", "moderate privacy level"),
    "INTERNAL":     ("moderate privacy level", "high privacy level"),
    "CONFIDENTIAL": ("high privacy level", "high privacy level"),
    "RESTRICTED":   ("high privacy level", "high privacy level"),
}
# Example: if a task requires the processing of data of level "INTERNAL", then the node must have a privacy level of at least "moderate" and at most "high" to be suitable for the task.

TASK_PRIORITY_REQUIREMENTS = {
    "LOW":          ("low reliability level"),
    "MEDIUM":       ("moderate reliability level"),
    "HIGH":         ("high reliability level"),
}
# Example: if a task has priority "HIGH", then the node must have a reliability level of at least "high" to be suitable for the task.


def local_llm_decide(state: dict, node_id: str, llm_tok, llm_mdl, task_type: str, reliability_level: str, privacy_level: str,
                                    in_data_privacy_lvl: str, out_data_privacy_lvl: str, task_priority: str) -> dict:
    print(f"{TAG} Running LLM decision for task of type {task_type}")

    cpu   = state["cpu"]      * 100
    mem   = state["mem"]      * 100
    disk  = state["disk"]     * 100
    cpu_p = state["cpu_pred"] * 100
    mem_p = state["mem_pred"] * 100
    score = state["score"]
    risk  = state["risk"]
    rep   = state["reputation"]
    rel   = state["reliability"]
    done  = state.get("tasks_completed", 0)

    def lvl(value, low, high): 
        return "high" if value>high else ("moderate" if value>low else "low")

    cpu_need, mem_need, desc = TASK_PROFILES.get(task_type, "GENERIC")
    min_in_privacy_lvl, max_in_privacy_lvl = TASK_DATA_PRIVACY_REQUIREMENTS.get(in_data_privacy_lvl, "CONFIDENTIAL")
    #TODO: min_out_privacy_lvl, max_out_privacy_lvl = TASK_DATA_PRIVACY_REQUIREMENTS.get(out_data_privacy_lvl, "PUBLIC")
    reliability_requirement = TASK_PRIORITY_REQUIREMENTS.get(task_priority, "MEDIUM")


    horizon = state.get("horizon", [])
    cpu_trend = mem_trend = "stable"
    if len(horizon) >= 2:
        cpu_trend = ("rising"  if horizon[-1][0]>horizon[0][0]+0.05 else
                     "falling" if horizon[-1][0]<horizon[0][0]-0.05 else "stable")
        mem_trend = ("rising"  if horizon[-1][1]>horizon[0][1]+0.03 else
                     "falling" if horizon[-1][1]<horizon[0][1]-0.03 else "stable")

    # TODO: think about this more...
    hard_reject = (score < ACCEPT_SCORE_MIN or risk=="CRITICAL"
                   or state.get("is_busy", False))
    
    # Build decision word SEPARATELY -- no nested f-string
    dw   = "REJECT" if hard_reject else "ACCEPT"
    rule = ("REJECT: score below threshold, CRITICAL risk, or node busy."
            if hard_reject else
            "ACCEPT: all thresholds met, node is available.")

    # JSON template as plain string concatenation
    json_tmpl = '{"decision": "' + dw + '", "reason": "<three sentences>"}'

    # Compute what makes this node specifically suitable or not
    cpu_gap   = cpu_p - cpu          # positive = CPU rising
    mem_gap   = mem_p - mem          # positive = memory rising
    score_gap = score - ACCEPT_SCORE_MIN

    # Task type-specific fit assessment
    if task_type == "CV_INFERENCE":
        fit_note = (f"CV_INFERENCE needs high CPU; current CPU={cpu:.1f}% "
                    f"predicted {cpu_p:.1f}% ({cpu_trend})")
    elif task_type == "CLASSIFICATION":
        fit_note = (f"CLASSIFICATION needs moderate CPU; current CPU={cpu:.1f}% "
                    f"({lvl(cpu,30,65)}), memory={mem:.1f}% ({lvl(mem,40,70)})")
    elif task_type == "TIMESERIES":
        fit_note = (f"TIMESERIES needs moderate CPU and memory; "
                    f"CPU={cpu:.1f}%({cpu_trend}), mem={mem:.1f}%({mem_trend})")
    else:
        fit_note = f"CPU={cpu:.1f}%, mem={mem:.1f}%, score={score:.4f}"

    # Task data privacy fit assessment
    privacy_fit_note = (f"Task requires AT LEAST {min_in_privacy_lvl} privacy level, "
                        f"and at most {max_in_privacy_lvl} privacy level; "
                        f"Node privacy level is {privacy_level} ")
    
    # Task priority fit assessment
    priority_fit_note = (f"Task with priority {task_priority} requires {reliability_requirement}; "
                         f"Node reliability level is {reliability_level}")

#    system_msg = (
#        "You are a concise edge-AI node policy engine. "
#        "Write exactly ONE sentence as the reason. "
#        "The sentence MUST: "
#        "(1) start with the task type name (e.g. 'CLASSIFICATION requires...'), "
#        "(2) include at least two specific numbers from the node state, "
#        "(3) explain the concrete fit or mismatch -- not just 'meets threshold'. "
#        "BAD example: 'All metrics meet requirements.' "
#        "GOOD example: 'CLASSIFICATION requires moderate CPU and this node shows "
#        "only 3.1% CPU (stable trend) with score 0.7205, well above the 0.5 threshold.' "
#        "Never start with I. Never be vague."
#    )

    system_msg = (
        "You are a concise edge-AI node policy engine. "
        "Write exactly THREE sentences as the reason. "
        "The reason MUST: "
        "(1) start with the task type name (e.g. 'CLASSIFICATION requires...') and information about the node's state, "
        "(2) continue with the privacy level of the node and the privacy requirement of the task,"
        "(3) conclude with the reliability level of the node and the reliability requirement of the task,"
        "(4) include at least two specific numbers from the node state, "
        "(5) explain the concrete fit or mismatch -- not just 'meets threshold'. "
        "BAD example: 'All metrics meet requirements.' "
        "GOOD example: 'CLASSIFICATION requires moderate CPU and this node shows "
        "only 3.1% CPU (stable trend) with score 0.7205, well above the 0.5 threshold. "
        "The node's privacy level is moderate, which meets the task's requirement of at least moderate privacy. "
        "The node's reliability level is high, which exceeds the task's requirement of medium reliability.' "
        "Never start with I. Never be vague."
    )

    user_msg = (
        f"Node {node_id} — decision for '{task_type}' task.\n"
        f"Task profile: {desc} — needs {cpu_need}, {mem_need}.\n\n"
        f"Node metrics:\n"
        f"  CPU now={cpu:.1f}%  predicted={cpu_p:.1f}%  trend={cpu_trend}\n"
        f"  Mem now={mem:.1f}%  predicted={mem_p:.1f}%  trend={mem_trend}\n"
        f"  Disk={disk:.1f}%\n"
        f"  Composite score={score:.4f} (threshold={ACCEPT_SCORE_MIN}, "
        f"margin={score_gap:+.4f})\n"
        f"  Risk={risk}  Rep={rep:.3f}  Rel={rel:.3f}\n"
        f"  Tasks completed={done}\n\n"
        f"Fit assessment: {fit_note}\n"
        f"Privacy assessment: {privacy_fit_note}\n"
        f"Reliability assessment: {priority_fit_note}\n"
        f"Decision: {rule}\n\n"
        f"Write the reason sentences — cite the specific numbers and information above.\n"
        f"Respond with ONLY this JSON:\n{json_tmpl}"
    )

    try:
        text   = llm_tok.apply_chat_template(
            [{"role":"system","content":system_msg},
             {"role":"user",  "content":user_msg}],
            tokenize=False, add_generation_prompt=True)
        inputs = llm_tok(text, return_tensors="pt")
        ilen   = inputs["input_ids"].shape[1]
        with torch.no_grad():
            out = llm_mdl.generate(**inputs, max_new_tokens=160,    # NOTE: previous max_new_tokens=80
                do_sample=False, pad_token_id=llm_tok.eos_token_id)
        raw = llm_tok.decode(out[0][ilen:], skip_special_tokens=True).strip()
        print(f"{TAG} LLM raw: {raw}")

        m = re.search(r'\{[^{}]*"decision"[^{}]*"reason"[^{}]*\}', raw, re.DOTALL)
        if m:
            p  = json.loads(m.group())
            d  = p.get("decision", dw).strip().upper()
            r  = p.get("reason", "").strip()
            if d not in ("ACCEPT","REJECT"): d = dw
            if r and len(r) > 15:
                return {"decision": d, "reason": r}
        print(f"{TAG} LLM JSON parse failed -- using fallback")
    except Exception as e:
        print(f"{TAG} LLM error: {e}\n{traceback.format_exc()}")

    # Meaningful fallback
    if dw == "ACCEPT":
        reason = (f"{TAG} {node_id} accepts {task_type}: CPU={cpu:.1f}%({cpu_trend}), "
                  f"score={score:.4f}>{ACCEPT_SCORE_MIN}, risk={risk}, "
                  f"{done} prior tasks completed.")
    else:
        reason = (f"{TAG} {node_id} rejects {task_type}: score={score:.4f} "
                  f"below {ACCEPT_SCORE_MIN} or risk={risk} or busy.")
    return {"decision": dw, "reason": reason}