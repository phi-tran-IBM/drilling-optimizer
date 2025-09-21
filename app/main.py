from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from app.agent.workflow import build_app, run_once

app = FastAPI(title="Well Planning Generator")

class PlanRequest(BaseModel):
    well_id: str
    objectives: Optional[str] = "Minimize cost and vibration while maintaining ROP"
    max_loops: int = 5

@app.post("/plan/run")
def plan_run(req: PlanRequest):
    graph = build_app()
    result = run_once(graph, well_id=req.well_id, objectives=req.objectives, max_loops=req.max_loops)
    return result
