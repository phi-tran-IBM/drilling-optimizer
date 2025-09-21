"""
Enhanced LangGraph Workflow for Knowledge-Driven Well Planning System

This module implements the core workflow orchestration for automated drilling plan generation
using LangGraph, supporting the Enterprise Knowledge Graph architecture and causal reasoning
approach described in the project documentation.

Key Features:
- Stateful workflow with comprehensive error handling
- GraphRAG context retrieval from Neo4j + AstraDB
- Iterative optimization with causal reflection
- Production-ready logging and monitoring
- Constraint satisfaction loop with KPI evaluation
- Markdown-based LLM generation for improved reliability

Author: Well Planning System
Version: 2.1.0
Dependencies: langgraph>=0.2, ibm-watsonx-ai>=1.2.0, pyyaml
"""

import os
import logging
from typing import TypedDict, List, Dict, Any, Optional
from uuid import uuid4

from langgraph.graph import StateGraph, END

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlanState(TypedDict):
    """
    State schema for the well planning workflow.
    
    This TypedDict defines the shared state that flows through all nodes in the 
    LangGraph workflow, implementing the knowledge-driven architecture described
    in the project documentation.
    """
    plan_id: str
    well_id: str
    context: Dict[str, Any]
    draft: str
    validation: Dict[str, Any]
    kpis: Dict[str, float]
    history: List[Dict[str, Any]]
    loop: int
    objectives: str
    max_loops: int
    error: Optional[str]
    parsed_plan: Optional[Dict[str, Any]]


def node_retrieve(state: PlanState) -> PlanState:
    """
    Retrieve contextual information for well planning using GraphRAG.
    
    This node implements the enhanced knowledge retrieval described in the project
    documentation, combining Neo4j graph traversal with vector search from AstraDB.
    
    Args:
        state: Current planning state
        
    Returns:
        Updated state with retrieved context
    """
    try:
        # Import here to avoid circular dependencies
        from app.graph.graph_rag import retrieve_subgraph_context
        
        logger.info(f"Retrieving context for well {state['well_id']}")
        
        # Enhanced context retrieval using GraphRAG
        objectives = state.get("objectives", "Minimize cost and vibration while maintaining ROP")
        ctx = retrieve_subgraph_context(state["well_id"], objectives)
        
        if not ctx:
            logger.warning(f"No context retrieved for well {state['well_id']}")
            ctx = {"objectives": objectives, "docs": [], "wells": [], "constraints": []}
        
        # Update state with retrieved context
        state = dict(state)  # Create a copy to avoid mutation issues
        state["context"] = ctx
        
        logger.info(f"Successfully retrieved context with {len(ctx.get('docs', []))} documents")
        return state
        
    except Exception as e:
        logger.error(f"Error in node_retrieve: {e}")
        state = dict(state)
        state["error"] = f"Context retrieval failed: {str(e)}"
        return state


def node_draft(state: PlanState) -> PlanState:
    """
    Generate drilling plan draft using watsonx.ai LLM with markdown formatting.
    
    This node creates an initial drilling plan based on the retrieved context,
    implementing the inference-engineering approach with markdown-structured prompting
    for improved LLM response reliability.
    
    Args:
        state: Current planning state with context
        
    Returns:
        Updated state with draft plan
    """
    try:
        # Import here to avoid circular dependencies
        from app.llm.watsonx_client import generate_drilling_plan_markdown, parse_markdown_plan
        
        logger.info(f"Generating draft plan for well {state['well_id']}")
        
        # Check for previous errors
        if state.get("error"):
            logger.warning(f"Skipping draft generation due to previous error: {state['error']}")
            return state
        
        # Get weight configuration for evidence sources
        graph_weight = float(os.getenv("GRAPH_WEIGHT", "0.7"))
        astra_weight = float(os.getenv("ASTRA_WEIGHT", "0.3"))
        
        # Extract context and objectives
        context = state.get("context", {})
        objectives = state.get("objectives", "Minimize cost and risk")
        
        # Generate markdown-formatted drilling plan
        markdown_plan = generate_drilling_plan_markdown(
            context=context,
            objectives=objectives,
            graph_weight=graph_weight,
            astra_weight=astra_weight
        )
        
        if not markdown_plan:
            raise ValueError("Empty response from LLM")
        
        # Parse markdown plan to structured data for compatibility
        parsed_plan = parse_markdown_plan(markdown_plan)
        
        # Validate parsing results
        if parsed_plan.get("parsing_error"):
            logger.warning(f"Plan parsing failed: {parsed_plan['parsing_error']}")
            logger.warning("Continuing with markdown text only")
        
        # Update state with both formats
        state = dict(state)
        state["draft"] = markdown_plan  # Store full markdown for human readability
        state["parsed_plan"] = parsed_plan  # Store structured data for processing
        
        logger.info(f"Successfully generated draft plan ({len(markdown_plan)} characters)")
        return state
        
    except Exception as e:
        logger.error(f"Error in node_draft: {e}")
        state = dict(state)
        state["error"] = f"Draft generation failed: {str(e)}"
        return state


def node_validate(state: PlanState) -> PlanState:
    """
    Validate drilling plan against engineering constraints and calculate KPIs.
    
    This node implements the constraint satisfaction loop using predictive models
    and the Risk-Cost Evaluator (RCE) as described in the project documentation.
    
    Args:
        state: Current planning state with draft plan
        
    Returns:
        Updated state with validation results and KPIs
    """
    try:
        # Import here to avoid circular dependencies
        from app.graph.graph_rag import validate_against_constraints, record_iteration
        from app.evaluation.kpi import compute_kpis
        
        logger.info(f"Validating plan for well {state['well_id']}, loop {state['loop']}")
        
        # Check for previous errors
        if state.get("error"):
            logger.warning(f"Skipping validation due to previous error: {state['error']}")
            return state
        
        if not state.get("draft"):
            raise ValueError("No draft plan available for validation")
        
        # Validate against engineering constraints using the markdown plan text
        validation_result = validate_against_constraints(state["well_id"], state["draft"])
        
        if not validation_result:
            logger.warning("Empty validation result - assuming failure")
            validation_result = {"passes": False, "violations": ["Unknown validation error"]}
        
        # Compute KPIs using the Risk-Cost Evaluator
        kpi_scores = compute_kpis(state["draft"], validation_result)
        
        if not kpi_scores:
            logger.warning("Empty KPI result - using defaults")
            kpi_scores = {"kpi_overall": 0.0, "kpi_risk": 1.0, "kpi_cost": 1.0}
        
        # Record this iteration for the knowledge graph
        record_iteration(
            plan_id=state["plan_id"],
            iteration=state["loop"],
            draft=state["draft"],
            validation=validation_result,
            kpis=kpi_scores
        )
        
        # Update state
        state = dict(state)
        state["validation"] = validation_result
        state["kpis"] = kpi_scores
        
        # Add to history for tracking
        history_entry = {
            "loop": state["loop"],
            "validation_passed": validation_result.get("passes", False),
            "kpi_score": kpi_scores.get("kpi_overall", 0.0),
            "timestamp": str(uuid4())  # Simple timestamp substitute
        }
        
        if "history" not in state:
            state["history"] = []
        state["history"].append(history_entry)
        
        passes = validation_result.get("passes", False)
        score = kpi_scores.get("kpi_overall", 0.0)
        logger.info(f"Validation complete - Passes: {passes}, Score: {score:.3f}")
        
        return state
        
    except Exception as e:
        logger.error(f"Error in node_validate: {e}")
        state = dict(state)
        state["error"] = f"Validation failed: {str(e)}"
        return state


def node_reflect(state: PlanState) -> PlanState:
    """
    Reflect on validation results and propose targeted improvements using markdown format.
    
    This node implements the causal reasoning approach described in the project
    documentation, using the knowledge graph to identify root causes and propose
    specific optimization strategies.
    
    Args:
        state: Current planning state with validation results
        
    Returns:
        Updated state with reflection and proposed changes
    """
    try:
        # Import here to avoid circular dependencies
        from app.llm.watsonx_client import generate_reflection_markdown, parse_markdown_reflection
        
        logger.info(f"Reflecting on validation results for well {state['well_id']}")
        
        # Check for previous errors
        if state.get("error"):
            logger.warning(f"Skipping reflection due to previous error: {state['error']}")
            return state
        
        validation = state.get("validation", {})
        
        # If validation passed, no reflection needed
        if validation.get("passes", False):
            logger.info("Validation passed - no reflection needed")
            return state
        
        # Extract validation failures and context
        violations = validation.get("violations", ["Unknown validation failure"])
        context = state.get("context", {})
        current_draft = state.get("draft", "")
        
        # Generate reflection using markdown format for better LLM performance
        reflection_response = generate_reflection_markdown(
            validation_failures=violations,
            current_plan=current_draft,
            context=context
        )
        
        if not reflection_response:
            logger.warning("Empty reflection response - using default")
            reflection_response = """### Proposed Change
**Change Type**: Parameter adjustment
**Specific Modification**: Review operational parameters
**Rationale**: Default fallback recommendation for failed validation"""
        
        # Parse the reflection for structured data if needed
        try:
            parsed_reflection = parse_markdown_reflection(reflection_response)
            if parsed_reflection.get("parsing_error"):
                logger.warning(f"Reflection parsing failed: {parsed_reflection['parsing_error']}")
        except Exception as e:
            logger.warning(f"Failed to parse reflection: {e}")
        
        # Append reflection to draft for next iteration
        state = dict(state)
        state["draft"] += f"\n\n## Iteration {state['loop']} Reflection\n{reflection_response}\n"
        
        logger.info(f"Reflection complete - proposed improvements added to draft")
        return state
        
    except Exception as e:
        logger.error(f"Error in node_reflect: {e}")
        state = dict(state)
        state["error"] = f"Reflection failed: {str(e)}"
        return state


def node_check(state: PlanState) -> PlanState:
    """
    Check loop conditions and prepare for next iteration or termination.
    
    This node manages the iterative optimization process, implementing convergence
    criteria and safeguards as described in the project documentation.
    
    Args:
        state: Current planning state
        
    Returns:
        Updated state with incremented loop counter
    """
    try:
        logger.info(f"Checking loop conditions for well {state['well_id']}")
        
        # Increment loop counter
        state = dict(state)
        state["loop"] += 1
        
        # Log current status
        validation = state.get("validation", {})
        passes = validation.get("passes", False)
        max_loops = state.get("max_loops", int(os.getenv("MAX_LOOPS", "5")))
        current_loop = state["loop"]
        
        logger.info(f"Loop {current_loop}/{max_loops} - Validation passed: {passes}")
        
        # Check for errors
        if state.get("error"):
            logger.warning(f"Terminating due to error: {state['error']}")
        
        return state
        
    except Exception as e:
        logger.error(f"Error in node_check: {e}")
        state = dict(state)
        state["error"] = f"Loop check failed: {str(e)}"
        return state


def should_continue(state: PlanState) -> str:
    """
    Conditional edge function to determine workflow continuation.
    
    This function implements the termination logic for the optimization loop,
    considering validation status, loop limits, and error conditions.
    
    Args:
        state: Current planning state
        
    Returns:
        Next node name or END
    """
    # Check for errors first
    if state.get("error"):
        logger.error(f"Terminating workflow due to error: {state['error']}")
        return END
    
    # Check validation status
    validation = state.get("validation", {})
    validation_passed = validation.get("passes", False)
    
    # Check loop limits
    max_loops = state.get("max_loops", int(os.getenv("MAX_LOOPS", "5")))
    current_loop = state.get("loop", 0)
    
    # Terminate if validation passed or max loops reached
    if validation_passed:
        logger.info("Workflow complete - validation passed")
        return END
    elif current_loop >= max_loops:
        logger.warning(f"Workflow complete - max loops ({max_loops}) reached")
        return END
    else:
        logger.info(f"Continuing workflow - loop {current_loop}/{max_loops}")
        return "draft"


def build_app() -> StateGraph:
    """
    Build and compile the LangGraph workflow for well planning.
    
    This function creates the stateful workflow graph implementing the 
    knowledge-driven architecture described in the project documentation.
    
    Returns:
        Compiled LangGraph application
    """
    try:
        logger.info("Building LangGraph workflow")
        
        # Create state graph with enhanced state schema
        workflow = StateGraph(PlanState)
        
        # Add nodes with descriptive names
        workflow.add_node("retrieve", node_retrieve)
        workflow.add_node("draft", node_draft)  
        workflow.add_node("validate", node_validate)
        workflow.add_node("reflect", node_reflect)
        workflow.add_node("check", node_check)
        
        # Set entry point
        workflow.set_entry_point("retrieve")
        
        # Add edges for the workflow
        workflow.add_edge("retrieve", "draft")
        workflow.add_edge("draft", "validate") 
        workflow.add_edge("validate", "reflect")
        workflow.add_edge("reflect", "check")
        
        # Add conditional edge for loop control
        workflow.add_conditional_edges(
            "check",
            should_continue,
            {
                "draft": "draft",
                END: END
            }
        )
        
        # Compile the workflow
        app = workflow.compile()
        
        logger.info("LangGraph workflow compiled successfully")
        return app
        
    except Exception as e:
        logger.error(f"Failed to build LangGraph workflow: {e}")
        raise


def run_once(
    app: StateGraph, 
    well_id: str, 
    objectives: str = "Minimize cost and vibration while maintaining ROP",
    max_loops: int = 5
) -> Dict[str, Any]:
    """
    Execute the well planning workflow once for a given well.
    
    This function runs the complete knowledge-driven well planning process,
    from context retrieval through iterative optimization to final plan generation.
    
    Args:
        app: Compiled LangGraph application
        well_id: Identifier for the target well
        objectives: Planning objectives and constraints
        max_loops: Maximum number of optimization iterations
        
    Returns:
        Dictionary containing plan results and metadata
    """
    try:
        logger.info(f"Starting well planning workflow for well {well_id}")
        
        # Set environment variable for this run
        os.environ["MAX_LOOPS"] = str(max_loops)
        
        # Initialize planning state
        initial_state: PlanState = {
            "plan_id": f"plan-{uuid4()}",
            "well_id": well_id,
            "context": {"objectives": objectives},
            "draft": "",
            "validation": {},
            "kpis": {},
            "history": [],
            "loop": 0,
            "objectives": objectives,
            "max_loops": max_loops,
            "error": None,
            "parsed_plan": None
        }
        
        logger.info(f"Initialized planning state with plan_id: {initial_state['plan_id']}")
        
        # Execute workflow
        final_state = app.invoke(initial_state)
        
        # Prepare result
        result = {
            "plan_id": final_state["plan_id"],
            "well_id": final_state["well_id"],
            "objectives": final_state["objectives"],
            "plan": final_state["draft"],
            "parsed_plan": final_state.get("parsed_plan"),
            "kpis": final_state["kpis"],
            "validation": final_state["validation"],
            "iterations": final_state["loop"],
            "max_loops": max_loops,
            "history": final_state.get("history", []),
            "success": final_state.get("validation", {}).get("passes", False),
            "error": final_state.get("error")
        }
        
        # Log completion
        success = result["success"]
        iterations = result["iterations"]
        logger.info(f"Workflow completed - Success: {success}, Iterations: {iterations}")
        
        return result
        
    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        return {
            "plan_id": f"plan-{uuid4()}",
            "well_id": well_id,
            "objectives": objectives,
            "plan": "",
            "parsed_plan": None,
            "kpis": {},
            "validation": {},
            "iterations": 0,
            "max_loops": max_loops,
            "history": [],
            "success": False,
            "error": str(e)
        }


# Expose the main functions for the application
__all__ = ["build_app", "run_once", "PlanState"]