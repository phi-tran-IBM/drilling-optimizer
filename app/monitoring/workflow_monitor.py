"""
LangGraph Monitoring and Debugging System

This module provides comprehensive monitoring, visualization, and debugging
capabilities for the LangGraph workflow, enabling transparency into agentic
reasoning and historical analysis of workflow efficacy.

Features:
- Real-time workflow state tracking
- Historical execution analysis
- Decision path visualization
- Performance metrics collection
- Audit trail generation
- Workflow efficacy validation

Author: Well Planning System
Version: 1.0.0
"""

import os
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class WorkflowMonitor:
    """Monitor and track LangGraph workflow execution for debugging and analysis."""
    
    def __init__(self, log_dir: str = "workflow_logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.current_execution = None
        
    def start_execution(self, plan_id: str, well_id: str, objectives: str) -> str:
        """Start monitoring a new workflow execution."""
        execution_id = f"{plan_id}_{int(time.time())}"
        
        self.current_execution = {
            "execution_id": execution_id,
            "plan_id": plan_id,
            "well_id": well_id,
            "objectives": objectives,
            "start_time": datetime.now().isoformat(),
            "nodes_executed": [],
            "state_transitions": [],
            "decisions": [],
            "errors": [],
            "performance_metrics": {}
        }
        
        logger.info(f"🔍 Started monitoring execution: {execution_id}")
        return execution_id
    
    def log_node_entry(self, node_name: str, state: Dict[str, Any]) -> None:
        """Log entry into a workflow node."""
        if not self.current_execution:
            return
            
        entry_time = datetime.now().isoformat()
        
        node_entry = {
            "node": node_name,
            "entry_time": entry_time,
            "loop_iteration": state.get("loop", 0),
            "state_snapshot": {
                "plan_id": state.get("plan_id"),
                "well_id": state.get("well_id"),
                "has_context": bool(state.get("context")),
                "has_draft": bool(state.get("draft")),
                "has_validation": bool(state.get("validation")),
                "has_kpis": bool(state.get("kpis")),
                "error": state.get("error")
            }
        }
        
        self.current_execution["nodes_executed"].append(node_entry)
        logger.info(f"📍 Node Entry: {node_name} (iteration {state.get('loop', 0)})")
    
    def log_node_exit(self, node_name: str, state: Dict[str, Any], 
                     processing_time: float) -> None:
        """Log exit from a workflow node with results."""
        if not self.current_execution:
            return
            
        # Find the corresponding entry
        for node_entry in reversed(self.current_execution["nodes_executed"]):
            if node_entry["node"] == node_name and "exit_time" not in node_entry:
                node_entry["exit_time"] = datetime.now().isoformat()
                node_entry["processing_time_seconds"] = processing_time
                node_entry["success"] = not bool(state.get("error"))
                
                # Add node-specific metrics
                if node_name == "retrieve":
                    node_entry["context_quality"] = {
                        "formations_count": len(state.get("context", {}).get("formations", [])),
                        "docs_count": len(state.get("context", {}).get("docs", [])),
                        "examples_count": len(state.get("context", {}).get("examples", []))
                    }
                elif node_name == "draft":
                    draft = state.get("draft", "")
                    node_entry["draft_quality"] = {
                        "length": len(draft),
                        "has_bha_section": "BHA Configuration" in draft,
                        "has_parameters": "Drilling Parameters" in draft,
                        "parsing_successful": not state.get("parsed_plan", {}).get("parsing_error")
                    }
                elif node_name == "validate":
                    validation = state.get("validation", {})
                    kpis = state.get("kpis", {})
                    node_entry["validation_results"] = {
                        "passes": validation.get("passes", False),
                        "violations_count": len(validation.get("violations", [])),
                        "confidence": validation.get("confidence", 0.0),
                        "kpi_overall": kpis.get("kpi_overall", 0.0)
                    }
                
                break
        
        logger.info(f"📍 Node Exit: {node_name} (processing time: {processing_time:.2f}s)")
    
    def log_decision(self, decision_point: str, decision: str, 
                    reasoning: str, state: Dict[str, Any]) -> None:
        """Log a decision made during workflow execution."""
        if not self.current_execution:
            return
            
        decision_entry = {
            "decision_point": decision_point,
            "decision": decision,
            "reasoning": reasoning,
            "timestamp": datetime.now().isoformat(),
            "loop_iteration": state.get("loop", 0),
            "context": {
                "validation_passed": state.get("validation", {}).get("passes", False),
                "violations_count": len(state.get("validation", {}).get("violations", [])),
                "current_loop": state.get("loop", 0),
                "max_loops": state.get("max_loops", 5)
            }
        }
        
        self.current_execution["decisions"].append(decision_entry)
        logger.info(f"🤔 Decision: {decision_point} → {decision} ({reasoning})")
    
    def log_error(self, node_name: str, error: str, state: Dict[str, Any]) -> None:
        """Log an error during workflow execution."""
        if not self.current_execution:
            return
            
        error_entry = {
            "node": node_name,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "loop_iteration": state.get("loop", 0),
            "state_at_error": {
                "has_context": bool(state.get("context")),
                "has_draft": bool(state.get("draft")),
                "has_validation": bool(state.get("validation"))
            }
        }
        
        self.current_execution["errors"].append(error_entry)
        logger.error(f"❌ Error in {node_name}: {error}")
    
    def finalize_execution(self, final_state: Dict[str, Any], 
                          total_execution_time: float) -> str:
        """Finalize monitoring and save execution log."""
        if not self.current_execution:
            return ""
            
        self.current_execution["end_time"] = datetime.now().isoformat()
        self.current_execution["total_execution_time"] = total_execution_time
        self.current_execution["final_results"] = {
            "success": final_state.get("validation", {}).get("passes", False),
            "iterations_completed": final_state.get("loop", 0),
            "final_kpi": final_state.get("kpis", {}).get("kpi_overall", 0.0),
            "error": final_state.get("error")
        }
        
        # Calculate performance metrics
        self.current_execution["performance_metrics"] = self._calculate_performance_metrics()
        
        # Save to file
        log_file = self.log_dir / f"{self.current_execution['execution_id']}.json"
        with open(log_file, 'w') as f:
            json.dump(self.current_execution, f, indent=2)
        
        execution_id = self.current_execution["execution_id"]
        self.current_execution = None
        
        logger.info(f"💾 Execution log saved: {log_file}")
        return str(log_file)
    
    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics for the execution."""
        nodes = self.current_execution["nodes_executed"]
        
        # Node execution times
        node_times = {}
        for node in nodes:
            if "processing_time_seconds" in node:
                node_name = node["node"]
                if node_name not in node_times:
                    node_times[node_name] = []
                node_times[node_name].append(node["processing_time_seconds"])
        
        # Average times per node
        avg_node_times = {
            node: sum(times) / len(times) 
            for node, times in node_times.items()
        }
        
        # Iteration analysis
        iterations = max([node.get("loop_iteration", 0) for node in nodes]) + 1
        
        return {
            "total_nodes_executed": len(nodes),
            "unique_nodes": len(set(node["node"] for node in nodes)),
            "iterations_completed": iterations,
            "avg_node_execution_times": avg_node_times,
            "total_decisions": len(self.current_execution["decisions"]),
            "total_errors": len(self.current_execution["errors"])
        }

class WorkflowAnalyzer:
    """Analyze historical workflow executions for efficacy validation."""
    
    def __init__(self, log_dir: str = "workflow_logs"):
        self.log_dir = Path(log_dir)
    
    def get_execution_history(self, well_id: Optional[str] = None, 
                            days: int = 30) -> List[Dict[str, Any]]:
        """Get historical executions, optionally filtered by well_id and time."""
        executions = []
        
        if not self.log_dir.exists():
            return executions
        
        cutoff_time = time.time() - (days * 24 * 60 * 60)
        
        for log_file in self.log_dir.glob("*.json"):
            try:
                with open(log_file, 'r') as f:
                    execution = json.load(f)
                
                # Filter by well_id if specified
                if well_id and execution.get("well_id") != well_id:
                    continue
                
                # Filter by time
                start_time = datetime.fromisoformat(execution["start_time"]).timestamp()
                if start_time < cutoff_time:
                    continue
                
                executions.append(execution)
                
            except Exception as e:
                logger.warning(f"Failed to load execution log {log_file}: {e}")
        
        return sorted(executions, key=lambda x: x["start_time"], reverse=True)
    
    def analyze_success_rates(self, executions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze success rates across executions."""
        if not executions:
            return {"error": "No executions found"}
        
        total = len(executions)
        successful = sum(1 for ex in executions 
                        if ex.get("final_results", {}).get("success", False))
        
        # Success by iteration count
        success_by_iterations = {}
        for ex in executions:
            iterations = ex.get("final_results", {}).get("iterations_completed", 0)
            if iterations not in success_by_iterations:
                success_by_iterations[iterations] = {"total": 0, "successful": 0}
            
            success_by_iterations[iterations]["total"] += 1
            if ex.get("final_results", {}).get("success", False):
                success_by_iterations[iterations]["successful"] += 1
        
        return {
            "overall_success_rate": successful / total,
            "total_executions": total,
            "successful_executions": successful,
            "success_by_iterations": success_by_iterations,
            "avg_iterations_to_success": self._avg_iterations_to_success(executions)
        }
    
    def analyze_performance_trends(self, executions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance trends over time."""
        if not executions:
            return {"error": "No executions found"}
        
        trends = {
            "execution_times": [],
            "kpi_scores": [],
            "iteration_counts": [],
            "success_trend": []
        }
        
        for ex in executions:
            trends["execution_times"].append(ex.get("total_execution_time", 0))
            trends["kpi_scores"].append(ex.get("final_results", {}).get("final_kpi", 0))
            trends["iteration_counts"].append(ex.get("final_results", {}).get("iterations_completed", 0))
            trends["success_trend"].append(ex.get("final_results", {}).get("success", False))
        
        return {
            "avg_execution_time": sum(trends["execution_times"]) / len(trends["execution_times"]),
            "avg_kpi_score": sum(trends["kpi_scores"]) / len(trends["kpi_scores"]),
            "avg_iterations": sum(trends["iteration_counts"]) / len(trends["iteration_counts"]),
            "recent_success_rate": sum(trends["success_trend"][:10]) / min(10, len(trends["success_trend"])),
            "performance_trends": trends
        }
    
    def identify_common_failure_patterns(self, executions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Identify common patterns in failed executions."""
        failed_executions = [ex for ex in executions 
                           if not ex.get("final_results", {}).get("success", False)]
        
        if not failed_executions:
            return {"message": "No failed executions found"}
        
        # Common error patterns
        error_patterns = {}
        node_failure_counts = {}
        
        for ex in failed_executions:
            # Count errors by node
            for error in ex.get("errors", []):
                node = error.get("node", "unknown")
                node_failure_counts[node] = node_failure_counts.get(node, 0) + 1
            
            # Count error messages
            final_error = ex.get("final_results", {}).get("error", "")
            if final_error:
                error_patterns[final_error] = error_patterns.get(final_error, 0) + 1
        
        return {
            "total_failed_executions": len(failed_executions),
            "most_common_errors": sorted(error_patterns.items(), key=lambda x: x[1], reverse=True)[:5],
            "nodes_with_most_failures": sorted(node_failure_counts.items(), key=lambda x: x[1], reverse=True),
            "avg_iterations_before_failure": sum(ex.get("final_results", {}).get("iterations_completed", 0) 
                                               for ex in failed_executions) / len(failed_executions)
        }
    
    def _avg_iterations_to_success(self, executions: List[Dict[str, Any]]) -> float:
        """Calculate average iterations for successful executions."""
        successful = [ex for ex in executions 
                     if ex.get("final_results", {}).get("success", False)]
        
        if not successful:
            return 0
        
        return sum(ex.get("final_results", {}).get("iterations_completed", 0) 
                  for ex in successful) / len(successful)

def create_workflow_dashboard(analyzer: WorkflowAnalyzer, well_id: Optional[str] = None) -> str:
    """Create a comprehensive workflow dashboard for monitoring."""
    executions = analyzer.get_execution_history(well_id=well_id, days=30)
    
    if not executions:
        return "No execution history found for analysis."
    
    success_analysis = analyzer.analyze_success_rates(executions)
    performance_trends = analyzer.analyze_performance_trends(executions)
    failure_patterns = analyzer.identify_common_failure_patterns(executions)
    
    dashboard = f"""
# LangGraph Workflow Dashboard
{'='*50}

## Overview
- **Total Executions**: {success_analysis['total_executions']}
- **Overall Success Rate**: {success_analysis['overall_success_rate']:.1%}
- **Average Execution Time**: {performance_trends['avg_execution_time']:.2f}s
- **Average KPI Score**: {performance_trends['avg_kpi_score']:.3f}

## Success Analysis
- **Successful Executions**: {success_analysis['successful_executions']}
- **Average Iterations to Success**: {success_analysis['avg_iterations_to_success']:.1f}
- **Recent Success Rate (last 10)**: {performance_trends['recent_success_rate']:.1%}

## Performance Trends
- **Average Iterations**: {performance_trends['avg_iterations']:.1f}
- **Execution Time Trend**: {'Improving' if len(performance_trends['performance_trends']['execution_times']) > 1 and performance_trends['performance_trends']['execution_times'][-1] < performance_trends['performance_trends']['execution_times'][0] else 'Stable'}

## Failure Analysis
{f"- **Failed Executions**: {failure_patterns['total_failed_executions']}" if 'total_failed_executions' in failure_patterns else "- **No Failures Found**"}
{f"- **Average Iterations Before Failure**: {failure_patterns['avg_iterations_before_failure']:.1f}" if 'avg_iterations_before_failure' in failure_patterns else ""}

### Most Common Errors:
"""
    
    if 'most_common_errors' in failure_patterns:
        for error, count in failure_patterns['most_common_errors']:
            dashboard += f"  - {error}: {count} times\n"
    else:
        dashboard += "  - No common errors found\n"
    
    dashboard += "\n### Nodes with Most Failures:\n"
    if 'nodes_with_most_failures' in failure_patterns:
        for node, count in failure_patterns['nodes_with_most_failures']:
            dashboard += f"  - {node}: {count} failures\n"
    else:
        dashboard += "  - No node failures found\n"
    
    return dashboard

# Save this as: app/monitoring/workflow_monitor.py
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="LangGraph Workflow Analysis")
    parser.add_argument("--well-id", help="Filter analysis by specific well ID")
    parser.add_argument("--days", type=int, default=30, help="Days of history to analyze")
    parser.add_argument("--action", choices=["dashboard", "history", "failures"], 
                       default="dashboard", help="Analysis action to perform")
    
    args = parser.parse_args()
    
    analyzer = WorkflowAnalyzer()
    
    if args.action == "dashboard":
        print(create_workflow_dashboard(analyzer, args.well_id))
    elif args.action == "history":
        executions = analyzer.get_execution_history(args.well_id, args.days)
        print(f"Found {len(executions)} executions")
        for ex in executions[:5]:  # Show last 5
            print(f"- {ex['execution_id']}: {ex['well_id']} - {'✅' if ex.get('final_results', {}).get('success') else '❌'}")
    elif args.action == "failures":
        executions = analyzer.get_execution_history(args.well_id, args.days)
        failure_patterns = analyzer.identify_common_failure_patterns(executions)
        print(json.dumps(failure_patterns, indent=2))