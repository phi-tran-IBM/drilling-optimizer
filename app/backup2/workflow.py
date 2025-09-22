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
- No fallback mechanisms - fails fast when dependencies unavailable

Author: Well Planning System
Version: 3.0.0
Dependencies: langgraph>=0.2, ibm-watsonx-ai>=1.2.0, neo4j>=5.16.0, astrapy>=1.4.2
"""

import os
import re
import time
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

# Environment validation at module level
def validate_workflow_environment():
    """Validate workflow-specific environment variables."""
    required_vars = [
        "WX_API_KEY", "WX_PROJECT_ID",  # Watson X AI
        "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD",  # Neo4j
        "ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN"  # AstraDB
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables for workflow: {missing}")

# Validate environment on import
validate_workflow_environment()

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
    retrieval_metadata: Optional[Dict[str, Any]]
    performance_metrics: Optional[Dict[str, Any]]


def node_retrieve(state: PlanState) -> PlanState:
    """
    Retrieve contextual information for well planning using GraphRAG.
    
    This node implements the enhanced knowledge retrieval described in the project
    documentation, combining Neo4j graph traversal with vector search from AstraDB.
    Fails fast if dependencies are unavailable.
    
    Args:
        state: Current planning state
        
    Returns:
        Updated state with retrieved context
        
    Raises:
        ConnectionError: If Neo4j or AstraDB connection fails
        ValueError: If no data found for well_id
    """
    try:
        # Import here to avoid circular dependencies
        from app.graph.graph_rag import retrieve_subgraph_context
        
        logger.info(f"Retrieving context for well {state['well_id']} with objectives: {state['objectives']}")
        
        # Enhanced context retrieval using GraphRAG
        objectives = state.get("objectives", "Minimize cost and vibration while maintaining ROP")
        
        # This will fail fast if Neo4j or AstraDB are unavailable
        ctx = retrieve_subgraph_context(state["well_id"], objectives)
        
        if not ctx:
            raise ValueError(f"No context retrieved for well {state['well_id']}")
        
        # Validate retrieved context quality
        formations_count = len(ctx.get("formations", []))
        docs_count = len(ctx.get("docs", []))
        
        if formations_count == 0:
            raise ValueError(f"No formation data found for well {state['well_id']}")
        
        if docs_count == 0:
            logger.warning(f"No documents found for well {state['well_id']} - may impact plan quality")
        
        # Update state with retrieved context
        state = dict(state)  # Create a copy to avoid mutation issues
        state["context"] = ctx
        state["retrieval_metadata"] = ctx.get("retrieval_metadata", {})
        
        logger.info(f"Successfully retrieved context: {formations_count} formations, {docs_count} docs, "
                   f"{len(ctx.get('examples', []))} historical examples")
        
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
    for improved LLM response reliability. Fails fast if LLM generation fails.
    
    Args:
        state: Current planning state with context
        
    Returns:
        Updated state with draft plan
        
    Raises:
        ValueError: If plan generation or parsing fails
        ConnectionError: If watsonx.ai API is unavailable
    """
    try:
        # Import here to avoid circular dependencies
        from app.llm.watsonx_client import generate_drilling_plan_markdown, parse_markdown_plan
        
        logger.info(f"Generating draft plan for well {state['well_id']}, iteration {state['loop']}")
        
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
        
        # Add iteration context for refinement
        if state.get("loop", 0) > 0:
            previous_validation = state.get("validation", {})
            if previous_validation.get("violations"):
                context["previous_violations"] = previous_validation["violations"]
                context["iteration_number"] = state["loop"]
        
        # Generate markdown-formatted drilling plan
        try:
            markdown_plan = generate_drilling_plan_markdown(
                context=context,
                objectives=objectives,
                graph_weight=graph_weight,
                astra_weight=astra_weight
            )
        except Exception as e:
            raise ConnectionError(f"watsonx.ai generation failed: {e}")
        
        if not markdown_plan or len(markdown_plan.strip()) < 100:
            raise ValueError("Generated plan is too short or empty")
        
        # Parse markdown plan to structured data for compatibility
        try:
            parsed_plan = parse_markdown_plan(markdown_plan)
        except Exception as e:
            raise ValueError(f"Plan parsing failed: {e}")
        
        # Validate parsing results
        if parsed_plan.get("parsing_error"):
            logger.warning(f"Plan parsing had issues: {parsed_plan['parsing_error']}")
            # Continue with markdown text only if parsing fails
        
        # Enhanced plan validation
        required_sections = ["Plan Summary", "BHA Configuration", "Drilling Parameters"]
        missing_sections = [section for section in required_sections 
                          if section not in markdown_plan]
        
        if missing_sections:
            logger.warning(f"Generated plan missing sections: {missing_sections}")
        
        # Update state with both formats
        state = dict(state)
        state["draft"] = markdown_plan  # Store full markdown for human readability
        state["parsed_plan"] = parsed_plan  # Store structured data for processing
        
        # Add performance metrics
        state["performance_metrics"] = {
            "plan_length": len(markdown_plan),
            "parsing_successful": not parsed_plan.get("parsing_error"),
            "sections_count": len(re.findall(r'^##\s+', markdown_plan, re.MULTILINE)),
            "generation_iteration": state["loop"]
        }
        
        logger.info(f"Successfully generated draft plan ({len(markdown_plan)} characters, "
                   f"parsing successful: {not parsed_plan.get('parsing_error')})")
        
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
    Uses real constraint validation against the knowledge graph.
    
    Args:
        state: Current planning state with draft plan
        
    Returns:
        Updated state with validation results and KPIs
        
    Raises:
        ValueError: If plan validation fails
        ConnectionError: If knowledge graph is unavailable
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
        
        # Validate against engineering constraints using the knowledge graph
        try:
            validation_result = validate_against_constraints(state["well_id"], state["draft"])
        except Exception as e:
            raise ConnectionError(f"Constraint validation failed: {e}")
        
        if not validation_result:
            raise ValueError("Empty validation result from constraint checker")
        
        # Enhanced validation result processing
        violations = validation_result.get("violations", [])
        violation_details = validation_result.get("violation_details", [])
        
        # Compute KPIs using the Risk-Cost Evaluator
        try:
            kpi_scores = compute_kpis(state["draft"], validation_result)
        except Exception as e:
            raise ValueError(f"KPI computation failed: {e}")
        
        if not kpi_scores:
            raise ValueError("Empty KPI result from evaluator")
        
        # Record this iteration in the knowledge graph for audit trail
        try:
            record_iteration(
                plan_id=state["plan_id"],
                iteration=state["loop"],
                draft=state["draft"],
                validation=validation_result,
                kpis=kpi_scores
            )
        except Exception as e:
            logger.warning(f"Failed to record iteration in knowledge graph: {e}")
            # Don't fail the validation if recording fails
        
        # Update state
        state = dict(state)
        state["validation"] = validation_result
        state["kpis"] = kpi_scores
        
        # Add to history for tracking
        history_entry = {
            "loop": state["loop"],
            "validation_passed": validation_result.get("passes", False),
            "kpi_score": kpi_scores.get("kpi_overall", 0.0),
            "violations_count": len(violations),
            "high_severity_violations": validation_result.get("high_severity_violations", 0),
            "confidence": validation_result.get("confidence", 0.0),
            "timestamp": str(uuid4())  # Simple timestamp substitute
        }
        
        if "history" not in state:
            state["history"] = []
        state["history"].append(history_entry)
        
        passes = validation_result.get("passes", False)
        score = kpi_scores.get("kpi_overall", 0.0)
        violations_count = len(violations)
        confidence = validation_result.get("confidence", 0.0)
        
        logger.info(f"Validation complete - Passes: {passes}, Score: {score:.3f}, "
                   f"Violations: {violations_count}, Confidence: {confidence:.2f}")
        
        if violations:
            logger.info(f"Constraint violations: {violations[:3]}...")  # Log first 3 violations
        
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
    specific optimization strategies. Only executes if validation failed.
    
    Args:
        state: Current planning state with validation results
        
    Returns:
        Updated state with reflection and proposed changes
        
    Raises:
        ValueError: If reflection generation fails
        ConnectionError: If LLM API is unavailable
    """
    try:
        # Import here to avoid circular dependencies
        from app.llm.watsonx_client import generate_reflection_markdown, parse_markdown_reflection
        
        logger.info(f"Reflecting on validation results for well {state['well_id']}, iteration {state['loop']}")
        
        # Check for previous errors
        if state.get("error"):
            logger.warning(f"Skipping reflection due to previous error: {state['error']}")
            return state
        
        validation = state.get("validation", {})
        
        # If validation passed, no reflection needed
        if validation.get("passes", False):
            logger.info("Validation passed - no reflection needed")
            return state
        
        # Extract validation failures and context for reflection
        violations = validation.get("violations", ["Unknown validation failure"])
        violation_details = validation.get("violation_details", [])
        context = state.get("context", {})
        current_draft = state.get("draft", "")
        
        # Enhanced reflection context with previous iterations
        reflection_context = {
            "current_plan": current_draft,
            "violations": violations,
            "violation_details": violation_details,
            "constraint_types_checked": validation.get("constraint_types_checked", []),
            "confidence": validation.get("confidence", 0.0),
            "iteration": state["loop"],
            "well_context": context,
            "previous_attempts": state.get("history", [])
        }
        
        # Generate reflection using markdown format for better LLM performance
        try:
            reflection_response = generate_reflection_markdown(
                validation_failures=violations,
                current_plan=current_draft,
                context=reflection_context
            )
        except Exception as e:
            raise ConnectionError(f"Reflection generation failed: {e}")
        
        if not reflection_response or len(reflection_response.strip()) < 50:
            raise ValueError("Generated reflection is too short or empty")
        
        # Parse the reflection for structured data if needed
        try:
            parsed_reflection = parse_markdown_reflection(reflection_response)
            if parsed_reflection.get("parsing_error"):
                logger.warning(f"Reflection parsing failed: {parsed_reflection['parsing_error']}")
        except Exception as e:
            logger.warning(f"Failed to parse reflection: {e}")
            parsed_reflection = {"reflection_text": reflection_response}
        
        # Append reflection to draft for next iteration with clear separation
        state = dict(state)
        reflection_section = f"""

---

## Iteration {state['loop']} - Constraint Violation Analysis

### Validation Results
- **Status**: FAILED
- **Violations Found**: {len(violations)}
- **Confidence**: {validation.get('confidence', 0.0):.2f}

### Root Cause Analysis and Optimization Strategy
{reflection_response}

---
"""
        
        state["draft"] += reflection_section
        
        # Store reflection metadata
        state["reflection_metadata"] = {
            "violations_analyzed": len(violations),
            "reflection_length": len(reflection_response),
            "parsing_successful": not parsed_reflection.get("parsing_error"),
            "iteration": state["loop"]
        }
        
        logger.info(f"Reflection complete - analyzed {len(violations)} violations, "
                   f"generated {len(reflection_response)} chars of analysis")
        
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
        Updated state with incremented loop counter and convergence analysis
    """
    try:
        logger.info(f"Checking loop conditions for well {state['well_id']}")
        
        # Increment loop counter
        state = dict(state)
        state["loop"] += 1
        
        # Enhanced status logging
        validation = state.get("validation", {})
        kpis = state.get("kpis", {})
        passes = validation.get("passes", False)
        max_loops = state.get("max_loops", int(os.getenv("MAX_LOOPS", "5")))
        current_loop = state["loop"]
        
        overall_kpi = kpis.get("kpi_overall", 0.0)
        violations_count = len(validation.get("violations", []))
        confidence = validation.get("confidence", 0.0)
        
        # Convergence analysis
        history = state.get("history", [])
        convergence_info = {}
        
        if len(history) > 1:
            # Check for improvement trends
            recent_scores = [h.get("kpi_score", 0.0) for h in history[-3:]]
            if len(recent_scores) >= 2:
                is_improving = recent_scores[-1] < recent_scores[-2]  # Lower KPI is better
                convergence_info["is_improving"] = is_improving
                convergence_info["score_trend"] = recent_scores
            
            # Check for oscillation (same violations repeating)
            recent_violation_counts = [h.get("violations_count", 0) for h in history[-2:]]
            if len(recent_violation_counts) == 2:
                convergence_info["is_oscillating"] = recent_violation_counts[0] == recent_violation_counts[1]
        
        state["convergence_info"] = convergence_info
        
        logger.info(f"Loop {current_loop}/{max_loops} - Validation: {passes}, "
                   f"KPI: {overall_kpi:.3f}, Violations: {violations_count}, "
                   f"Confidence: {confidence:.2f}")
        
        if convergence_info.get("is_improving") is not None:
            logger.info(f"Convergence trend - Improving: {convergence_info['is_improving']}, "
                       f"Oscillating: {convergence_info.get('is_oscillating', False)}")
        
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
    considering validation status, loop limits, error conditions, and convergence.
    
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
    
    # Check convergence conditions
    convergence_info = state.get("convergence_info", {})
    is_oscillating = convergence_info.get("is_oscillating", False)
    
    # Enhanced termination logic
    if validation_passed:
        logger.info("Workflow complete - validation passed ✅")
        return END
    elif current_loop >= max_loops:
        logger.warning(f"Workflow complete - max loops ({max_loops}) reached ⚠️")
        return END
    elif is_oscillating and current_loop >= 3:
        logger.warning("Workflow complete - oscillation detected, preventing infinite loop ⚠️")
        return END
    else:
        violations_count = len(validation.get("violations", []))
        kpi_score = state.get("kpis", {}).get("kpi_overall", 0.0)
        
        logger.info(f"Continuing workflow - loop {current_loop}/{max_loops}, "
                   f"{violations_count} violations, KPI: {kpi_score:.3f} 🔄")
        return "draft"


def build_app() -> StateGraph:
    """
    Build and compile the LangGraph workflow for well planning.
    
    This function creates the stateful workflow graph implementing the 
    knowledge-driven architecture described in the project documentation.
    Includes comprehensive error handling and monitoring.
    
    Returns:
        Compiled LangGraph application
        
    Raises:
        EnvironmentError: If required dependencies are not available
    """
    try:
        logger.info("Building LangGraph workflow with knowledge-driven architecture")
        
        # Validate that all required modules are available
        try:
            from app.graph.graph_rag import retrieve_subgraph_context
            from app.llm.watsonx_client import generate_drilling_plan_markdown
            from app.evaluation.kpi import compute_kpis
        except ImportError as e:
            raise EnvironmentError(f"Required module not available: {e}")
        
        # Create state graph with enhanced state schema
        workflow = StateGraph(PlanState)
        
        # Add nodes with descriptive names and error handling
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
        
        logger.info("LangGraph workflow compiled successfully ✅")
        return app
        
    except Exception as e:
        logger.error(f"Failed to build LangGraph workflow: {e}")
        raise


def run_once(
    app: StateGraph, 
    well_id: str, 
    objectives: str = "Minimize cost and vibration while maintaining ROP",
    max_loops: int = 5,
    enable_monitoring: bool = True
) -> Dict[str, Any]:
    """
    Execute the well planning workflow once for a given well.
    
    This function runs the complete knowledge-driven well planning process,
    from context retrieval through iterative optimization to final plan generation.
    Includes comprehensive error handling and performance monitoring.
    
    Args:
        app: Compiled LangGraph application
        well_id: Identifier for the target well
        objectives: Planning objectives and constraints
        max_loops: Maximum number of optimization iterations
        
    Returns:
        Dictionary containing plan results and metadata
        
    Raises:
        ValueError: If input parameters are invalid
        EnvironmentError: If required services are unavailable
    """
    if not well_id or not isinstance(well_id, str):
        raise ValueError("well_id must be a non-empty string")
    
    if max_loops < 1 or max_loops > 20:
        raise ValueError("max_loops must be between 1 and 20")
    
    try:
        logger.info(f"Starting knowledge-driven well planning workflow for well {well_id}")
        logger.info(f"Objectives: {objectives}")
        logger.info(f"Max iterations: {max_loops}")
        
        # Initialize monitoring if enabled
        monitor = None
        if enable_monitoring:
            try:
                from app.monitoring.workflow_monitor import WorkflowMonitor
                monitor = WorkflowMonitor()
            except ImportError:
                logger.warning("Monitoring module not available - continuing without monitoring")
                monitor = None
        
        # Set environment variable for this run
        os.environ["MAX_LOOPS"] = str(max_loops)
        
        # Initialize planning state with enhanced metadata
        plan_id = f"plan-{uuid4()}"
        
        if monitor:
            execution_id = monitor.start_execution(plan_id, well_id, objectives)
            logger.info(f"Monitoring enabled - execution ID: {execution_id}")
        
        initial_state: PlanState = {
            "plan_id": plan_id,
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
            "parsed_plan": None,
            "retrieval_metadata": None,
            "performance_metrics": None
        }
        
        # Add monitor to state for node access
        if monitor:
            initial_state["_monitor"] = monitor
        
        logger.info(f"Initialized planning state with plan_id: {plan_id}")
        
        # Execute workflow with timing
        start_time = time.time()
        
        final_state = app.invoke(initial_state)
        
        execution_time = time.time() - start_time
        
        # Finalize monitoring
        log_file = None
        if monitor:
            log_file = monitor.finalize_execution(final_state, execution_time)
        
        # Prepare comprehensive result
        success = final_state.get("validation", {}).get("passes", False)
        iterations = final_state["loop"]
        kpis = final_state.get("kpis", {})
        validation = final_state.get("validation", {})
        
        result = {
            "plan_id": final_state["plan_id"],
            "well_id": final_state["well_id"],
            "objectives": final_state["objectives"],
            "plan": final_state["draft"],
            "parsed_plan": final_state.get("parsed_plan"),
            "kpis": kpis,
            "validation": validation,
            "iterations": iterations,
            "max_loops": max_loops,
            "history": final_state.get("history", []),
            "success": success,
            "error": final_state.get("error"),
            "execution_time_seconds": round(execution_time, 2),
            "retrieval_metadata": final_state.get("retrieval_metadata"),
            "performance_metrics": final_state.get("performance_metrics"),
            "convergence_info": final_state.get("convergence_info"),
            "timestamp": time.time(),
            "monitoring_log": log_file
        }
        
        # Enhanced logging
        if success:
            logger.info(f"✅ Workflow completed successfully!")
        else:
            logger.warning(f"⚠️ Workflow completed without full validation")
        
        logger.info(f"📊 Final Results:")
        logger.info(f"   - Iterations: {iterations}/{max_loops}")
        logger.info(f"   - Validation passed: {success}")
        logger.info(f"   - KPI overall: {kpis.get('kpi_overall', 'N/A')}")
        logger.info(f"   - Violations: {len(validation.get('violations', []))}")
        logger.info(f"   - Execution time: {execution_time:.2f}s")
        
        if final_state.get("error"):
            logger.error(f"   - Error: {final_state['error']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        
        # Return error result with partial state if available
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
            "error": str(e),
            "execution_time_seconds": 0,
            "retrieval_metadata": None,
            "performance_metrics": None,
            "convergence_info": None,
            "timestamp": time.time()
        }


# Expose the main functions for the application
__all__ = ["build_app", "run_once", "PlanState"]