import json
import re
import traceback
import torch

ACCEPT_SCORE_MIN = 0.50
TAG = "[LLM]"

# NOTE: Do we still need the task profiles?

TASK_PROFILES = {
    "CLASSIFICATION": ("moderate CPU", "low memory",      "ML classification"),
    "CV_INFERENCE":   ("high CPU",     "moderate memory", "computer vision"),
    "TIMESERIES":     ("moderate CPU", "moderate memory", "time-series LSTM"),
    "PRIVATE_TASK":   ("moderate CPU", "moderate memory", "general compute"),   # NOTE. GENERIC
}


def local_llm_decide(state: dict, node_id: str, llm_tok, llm_mdl, task_type: str = "GENERIC") -> dict:
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

    def lvl(v, lo, hi): 
        return "high" if v>hi else ("moderate" if v>lo else "low")

    cpu_need, mem_need, desc = TASK_PROFILES.get(task_type, TASK_PROFILES["GENERIC"])

    horizon = state.get("horizon", [])
    cpu_trend = mem_trend = "stable"
    if len(horizon) >= 2:
        cpu_trend = ("rising"  if horizon[-1][0]>horizon[0][0]+0.05 else
                     "falling" if horizon[-1][0]<horizon[0][0]-0.05 else "stable")
        mem_trend = ("rising"  if horizon[-1][1]>horizon[0][1]+0.03 else
                     "falling" if horizon[-1][1]<horizon[0][1]-0.03 else "stable")

    hard_reject = (score < ACCEPT_SCORE_MIN or risk=="CRITICAL"
                   or state.get("is_busy", False))

    # Build decision word SEPARATELY -- no nested f-string
    dw   = "REJECT" if hard_reject else "ACCEPT"
    rule = ("REJECT: score below threshold, CRITICAL risk, or node busy."
            if hard_reject else
            "ACCEPT: all thresholds met, node is available.")

    # JSON template as plain string concatenation
    json_tmpl = '{"decision": "' + dw + '", "reason": "<one sentence>"}'

    # Compute what makes this node specifically suitable or not
    cpu_gap   = cpu_p - cpu          # positive = CPU rising
    mem_gap   = mem_p - mem          # positive = memory rising
    score_gap = score - ACCEPT_SCORE_MIN

    # Task-specific fit assessment
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

    system_msg = (
        "You are a concise edge-AI node policy engine. "
        "Write exactly ONE sentence as the reason. "
        "The sentence MUST: "
        "(1) start with the task type name (e.g. 'CLASSIFICATION requires...'), "
        "(2) include at least two specific numbers from the node state, "
        "(3) explain the concrete fit or mismatch -- not just 'meets threshold'. "
        "BAD example: 'All metrics meet requirements.' "
        "GOOD example: 'CLASSIFICATION requires moderate CPU and this node shows "
        "only 3.1% CPU (stable trend) with score 0.7205, well above the 0.5 threshold.' "
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
        f"Decision: {rule}\n\n"
        f"Write the reason sentence — cite the specific numbers above.\n"
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
            out = llm_mdl.generate(**inputs, max_new_tokens=80,
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