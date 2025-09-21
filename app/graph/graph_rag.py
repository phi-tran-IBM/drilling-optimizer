import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv("NEO4J_URI"); USER = os.getenv("NEO4J_USER"); PWD = os.getenv("NEO4J_PASSWORD")

def _session():
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    return driver.session()

def _astra_snippets(keywords: str, k: int = 3):
    """Retrieve k short snippets from AstraDB using server-side vectorize ONLY."""
    from astrapy import DataAPIClient
    client = DataAPIClient()
    db = client.get_database(os.getenv("ASTRA_DB_API_ENDPOINT"), token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
    coll = db.get_collection(os.getenv("ASTRA_DB_VECTOR_COLLECTION", "drilling_docs"))
    use_vec = os.getenv("ASTRA_USE_SERVER_VECTORIZE", "false").lower() in ["1","true","yes"]
    if not use_vec:
        raise RuntimeError("ASTRA_USE_SERVER_VECTORIZE must be true and vectorize must be enabled on the collection.")
    cur = coll.find({}, options={"limit": k, "includeSimilarity": True, "sort": {"$vectorize": keywords}})
    items = list(cur)
    snippets = []
    for d in items:
        body = d.get("body","")
        src = d.get("path") or d.get("url")
        if body:
            snippet = body[:500] + ("..." if len(body) > 500 else "")
        else:
            snippet = "(no text extracted)"
        snippets.append({"source": src, "snippet": snippet})
    return snippets

def retrieve_subgraph_context(well_id: str, objectives: str) -> dict:
    q = """
MATCH (w:Well {well_id:$wid})-[:HAS_FORMATION]->(f:Formation)
WITH w, collect({name:f.name, depth:[f.depth_start,f.depth_end], rs:f.rock_strength, pp:f.pore_pressure}) AS formations
OPTIONAL MATCH (hp:HistoricalPlan)<-[:HAS_PLAN]-(w)
WITH w, formations, collect({plan_id:hp.plan_id, kpi:hp.final_kpi_score})[0..3] AS examples
OPTIONAL MATCH (b:BHATool)-[:HAS_CONSTRAINT]->(c:EngineeringConstraint)
WITH w, formations, examples, collect({part:b.part_number, type:b.tool_type, limits:c{.*}}) AS bha
RETURN {well_id:w.well_id, formations:formations, examples:examples, bha:bha} AS ctx
"""
    with _session() as s:
        rec = s.run(q, wid=well_id).single()
        ctx = rec["ctx"] if rec else {"well_id": well_id}
        ctx["objectives"] = objectives
        form_names = ", ".join([f.get("name","") for f in ctx.get("formations", [])])
        keywords = (form_names + " " + objectives).strip() or well_id
        ctx["docs"] = _astra_snippets(keywords, k=3)
        return ctx

def validate_against_constraints(well_id: str, plan_text: str) -> dict:
    import random
    v = {"passes": random.random() > 0.4, "violations": 0, "details": "demo"}
    if not v["passes"]:
        v["violations"] = 1
    return v

def record_iteration(plan_id: str, loop: int, draft: str, validation: dict, kpis: dict):
    q = """MERGE (p:PlanIteration {plan_id:$pid, iter:$iter}) SET p.draft=$draft, p.validation=$validation, p.kpis=$kpis, p.ts=timestamp()"""
    with _session() as s:
        s.run(q, pid=plan_id, iter=loop, draft=draft, validation=validation, kpis=kpis)
