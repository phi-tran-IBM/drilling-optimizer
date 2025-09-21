import os
from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END
from app.llm.watsonx_client import llm_generate
from app.graph.graph_rag import retrieve_subgraph_context, record_iteration, validate_against_constraints
from app.evaluation.kpi import compute_kpis

class PlanState(TypedDict):
    plan_id: str
    well_id: str
    context: Dict[str, Any]
    draft: str
    validation: Dict[str, Any]
    kpis: Dict[str, float]
    history: List[Dict[str, Any]]
    loop: int

def node_retrieve(state: PlanState) -> PlanState:
    ctx = retrieve_subgraph_context(state["well_id"], state.get("context", {}).get("objectives", ""))
    state["context"] = ctx
    return state

def node_draft(state: PlanState) -> PlanState:
    graph_w = float(os.getenv("GRAPH_WEIGHT", "0.7"))
    astra_w = float(os.getenv("ASTRA_WEIGHT", "0.3"))
    doc_bits = state.get("context", {}).get("docs", [])
    weight_note = f"Evidence weights → Graph:{graph_w:.2f} Astra:{astra_w:.2f}"
    structured_ctx = {k: v for k, v in state["context"].items() if k != "docs"}
    prompt = f"""You are a drilling planning assistant.
{weight_note}

OBJECTIVES:
{state.get('context',{}).get('objectives','Minimize cost and risk')}

GRAPH CONTEXT (weight={graph_w}):
{structured_ctx}

ASTRA DOC SNIPPETS (weight={astra_w}):
{doc_bits}

Draft a concise drilling plan with BHA, section parameters (WOB, RPM, Flow), and mitigations for risks.
Return JSON with keys: plan_text, parameters (array), expected_risks.
"""
    text = llm_generate(prompt)
    state["draft"] = text
    return state

def node_validate(state: PlanState) -> PlanState:
    v = validate_against_constraints(state["well_id"], state["draft"])
    k = compute_kpis(state["draft"], v)
    state["validation"] = v
    state["kpis"] = k
    record_iteration(state["plan_id"], state["loop"], state["draft"], v, k)
    return state

def node_reflect(state: PlanState) -> PlanState:
    v = state["validation"]
    if v.get("passes", False):
        return state
    prompt = f"""The last plan failed validation:
{v}

Propose ONE targeted change (bit/BHA or parameter) most likely to fix the top violation while preserving ROP.
Return a short JSON with keys: change, rationale.

CONTEXT:
{state['context']}

PLAN:
{state['draft']}
"""
    suggestion = llm_generate(prompt)
    state["draft"] += f"\n\n# Reflection Change\n{suggestion}\n"
    return state

def node_check(state: PlanState) -> PlanState:
    state["loop"] += 1
    return state

def build_app():
    g = StateGraph(PlanState)
    g.add_node("retrieve", node_retrieve)
    g.add_node("draft", node_draft)
    g.add_node("validate", node_validate)
    g.add_node("reflect", node_reflect)
    g.add_node("check", node_check)

    g.set_entry_point("retrieve")
    g.add_edge("retrieve", "draft")
    g.add_edge("draft", "validate")
    g.add_edge("validate", "reflect")
    g.add_edge("reflect", "check")
    g.add_conditional_edges("check", lambda s: END if s["validation"].get("passes", False) or s["loop"]>=int(os.getenv("MAX_LOOPS","5")) else "draft", {"draft":"draft"})
    return g.compile()

def run_once(app, well_id: str, objectives: str, max_loops: int = 5):
    from uuid import uuid4
    os.environ["MAX_LOOPS"] = str(max_loops)
    plan_state: PlanState = {
        "plan_id": f"plan-{uuid4()}",
        "well_id": well_id,
        "context": {"objectives": objectives},
        "draft": "",
        "validation": {},
        "kpis": {},
        "history": [],
        "loop": 0
    }
    final = app.invoke(plan_state)
    return {"plan_id": final["plan_id"], "kpis": final["kpis"], "validation": final["validation"], "plan": final["draft"]}
