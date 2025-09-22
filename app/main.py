# app/main.py
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import Optional, Literal
from app.core.config import settings
from app.agent.workflow import build_app, run_once

# Import new components
from app.retrieval.strategies import (
    ConstraintFirstStrategy,
    HybridGraphRAGStrategy
)

app = FastAPI(
    title="Well Planning Knowledge System",
    version="2.0.0",
    description="Knowledge-Driven Well Planning with Multiple Retrieval Strategies"
)

class PlanRequest(BaseModel):
    well_id: str
    objectives: Optional[str] = "Minimize cost and vibration while maintaining ROP"
    max_loops: int = 5
    retrieval_strategy: Literal["constraint_first", "hybrid", "legacy"] = "legacy"

class HealthCheck(BaseModel):
    status: str
    environment: str
    monitoring_enabled: bool
    multi_agent_enabled: bool
    available_strategies: list

@app.get("/health", response_model=HealthCheck)
def health_check():
    """Enhanced health check with configuration info"""
    return HealthCheck(
        status="healthy",
        environment=settings.environment,
        monitoring_enabled=settings.enable_monitoring,
        multi_agent_enabled=settings.enable_multi_agent,
        available_strategies=["legacy", "constraint_first", "hybrid"]
    )

@app.post("/plan/run")
def plan_run(req: PlanRequest):
    """Original endpoint - maintains backward compatibility"""
    graph = build_app()
    result = run_once(
        graph, 
        well_id=req.well_id, 
        objectives=req.objectives, 
        max_loops=req.max_loops
    )
    return result

@app.post("/plan/run/v2")
def plan_run_v2(req: PlanRequest):
    """New endpoint with strategy selection"""
    # Select retrieval strategy
    if req.retrieval_strategy == "constraint_first":
        strategy = ConstraintFirstStrategy()
    elif req.retrieval_strategy == "hybrid":
        strategy = HybridGraphRAGStrategy()
    else:
        strategy = None  # Use legacy
    
    # Build workflow with selected strategy
    graph = build_app(retrieval_strategy=strategy)
    
    result = run_once(
        graph,
        well_id=req.well_id,
        objectives=req.objectives,
        max_loops=req.max_loops
    )
    
    # Add strategy metadata to response
    result["retrieval_strategy_used"] = req.retrieval_strategy
    
    return result