import os
from dotenv import load_dotenv
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams
import re

load_dotenv()

# Environment validation for watsonx client - STRICT MODE
def validate_watsonx_environment():
    """Validate watsonx environment variables - NO FALLBACKS."""
    required_vars = ["WX_API_KEY", "WX_PROJECT_ID", "WX_URL", "WX_MODEL_ID"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required watsonx environment variables: {missing}")

# Call validation on module import - CRITICAL FOR FAIL-FAST
validate_watsonx_environment()

def llm_generate(prompt: str) -> str:
    """
    Generate text using IBM WatsonX foundation models for drilling engineering tasks.
    STRICT MODE - No fallbacks, no graceful degradation.
    
    Args:
        prompt: Input prompt for text generation
        
    Returns:
        Generated text response
        
    Raises:
        ValueError: If prompt is invalid or generation fails
        EnvironmentError: If credentials are missing  
        ConnectionError: If API is unavailable
    """
    # STRICT input validation
    if not prompt:
        raise ValueError("Prompt cannot be empty")
    
    if not isinstance(prompt, str):
        raise ValueError(f"Prompt must be string, got: {type(prompt)}")
    
    if len(prompt.strip()) < 10:
        raise ValueError(f"Prompt too short: {len(prompt)} characters")
    
    # Get environment variables - STRICT validation
    model_id = os.environ["WX_MODEL_ID"]
    api_key = os.environ["WX_API_KEY"]
    url = os.environ["WX_URL"]
    project_id = os.environ["WX_PROJECT_ID"]
    
    # STRICT parameter configuration
    params = {
        GenParams.MAX_NEW_TOKENS: 3000,
        GenParams.TEMPERATURE: 0.5,
        GenParams.DECODING_METHOD: "greedy",
        GenParams.REPETITION_PENALTY: 1.05,
        GenParams.TOP_P: 0.9,
        GenParams.TOP_K: 50
    }
    
    try:
        model = ModelInference(
            model_id=model_id,
            params=params,
            credentials={"url": url, "apikey": api_key},
            project_id=project_id
        )
        
        response = model.generate_text(prompt=prompt)
        
    except Exception as e:
        raise ConnectionError(f"WatsonX API call failed: {e}")
    
    # STRICT response processing
    if response is None:
        raise ValueError("WatsonX returned None response")
    
    if isinstance(response, str):
        generated_text = response
    elif isinstance(response, dict):
        results = response.get("results")
        if not results:
            raise ValueError("WatsonX response missing 'results' field")
        if not isinstance(results, list) or len(results) == 0:
            raise ValueError("WatsonX response 'results' field is empty")
        
        generated_text = results[0].get("generated_text")
        if generated_text is None:
            raise ValueError("WatsonX response missing 'generated_text' field")
    else:
        raise ValueError(f"Unexpected WatsonX response type: {type(response)}")
    
    # STRICT content validation
    if not isinstance(generated_text, str):
        raise ValueError(f"Generated text must be string, got: {type(generated_text)}")
    
    # Content validation for drilling engineering context
    if any(unwanted in generated_text.lower() for unwanted in ["#include", "void main", "<stdio.h>", "function main"]):
        raise ValueError("Model generated programming code instead of drilling engineering content")
    
    if len(generated_text.strip()) < 3:
        raise ValueError("Generated response is too short")
    
    return generated_text.strip()


def _validate_context_structure(context: dict) -> None:
    """
    Validate that context contains all required keys - fail fast if missing
    """
    required_keys = ["docs", "formations", "well_info", "offset_wells", "historical_performance"]
    missing_keys = [key for key in required_keys if key not in context]
    if missing_keys:
        raise KeyError(f"Context missing required keys: {missing_keys}")


def generate_drilling_plan_markdown(context: dict, objectives: str, graph_weight: float = 0.7, astra_weight: float = 0.3) -> str:
    """
    Generate a comprehensive drilling plan using markdown formatting optimized for LLM generation.
    STRICT MODE - No fallbacks, comprehensive validation.
    
    Args:
        context: Well and geological context data from GraphRAG
        objectives: Planning objectives and constraints
        graph_weight: Weight for graph-based evidence
        astra_weight: Weight for document-based evidence
        
    Returns:
        Generated drilling plan in markdown format
        
    Raises:
        ValueError: If inputs are invalid
        ConnectionError: If LLM generation fails
    """
    # STRICT input validation
    if not context:
        raise ValueError("Context cannot be empty")
    
    if not isinstance(context, dict):
        raise ValueError(f"Context must be dict, got: {type(context)}")
    
    # Validate context structure - fail fast if required keys missing
    _validate_context_structure(context)
    
    if not objectives:
        raise ValueError("Objectives cannot be empty")
    
    if not isinstance(objectives, str):
        raise ValueError(f"Objectives must be string, got: {type(objectives)}")
    
    if not isinstance(graph_weight, (int, float)):
        raise ValueError(f"Graph weight must be numeric, got: {type(graph_weight)}")
    
    if not isinstance(astra_weight, (int, float)):
        raise ValueError(f"Astra weight must be numeric, got: {type(astra_weight)}")
    
    if graph_weight < 0 or graph_weight > 1:
        raise ValueError(f"Graph weight must be 0-1, got: {graph_weight}")
    
    if astra_weight < 0 or astra_weight > 1:
        raise ValueError(f"Astra weight must be 0-1, got: {astra_weight}")
    
    # Extract context components with STRICT validation
    docs = context["docs"]
    if not isinstance(docs, list):
        raise ValueError(f"Context docs must be list, got: {type(docs)}")
    
    formations = context["formations"]
    if not isinstance(formations, list):
        raise ValueError(f"Context formations must be list, got: {type(formations)}")
    
    if len(formations) == 0:
        raise ValueError("Context formations list is empty")
    
    # Extract additional context with strict access
    well_info = context["well_info"]
    if not isinstance(well_info, dict):
        raise ValueError(f"Context well_info must be dict, got: {type(well_info)}")
    
    offset_wells = context["offset_wells"]
    if not isinstance(offset_wells, list):
        raise ValueError(f"Context offset_wells must be list, got: {type(offset_wells)}")
    
    historical_performance = context["historical_performance"]
    if not isinstance(historical_performance, dict):
        raise ValueError(f"Context historical_performance must be dict, got: {type(historical_performance)}")
    
    # Build weight annotation for transparency
    weight_note = f"Evidence weights → Graph:{graph_weight:.2f} Astra:{astra_weight:.2f}"
    
    # Format context information with STRICT validation
    well_info_text = f"Well ID: {well_info.get('well_id', 'Unknown')}, Location: {well_info.get('location', 'Unknown')}"
    
    formations_text = ""
    try:
        formations_text = "\n".join([
            f"- {f.get('name', 'Unknown')}: {f.get('depth', [0, 0])[0]}-{f.get('depth', [0, 0])[1]} ft"
            for f in formations[:3]
        ])
    except (KeyError, IndexError, TypeError) as e:
        raise ValueError(f"Invalid formation data structure: {e}")
    
    if not formations_text:
        raise ValueError("Failed to format formations text")
    
    # Process documents with validation
    doc_snippets = []
    for i, doc in enumerate(docs[:2]):
        if not isinstance(doc, dict):
            continue
        snippet = doc.get("snippet", "")
        if snippet and isinstance(snippet, str):
            doc_snippets.append(f"Document {i+1}: {snippet[:200]}...")
    
    if not doc_snippets:
        raise ValueError("No valid document snippets found")
    
    docs_text = "\n".join(doc_snippets)
    
    # Enhanced prompt with simplified structure for better generation
    prompt = f"""You are an expert drilling engineer. Create a detailed drilling plan.

{weight_note}

OBJECTIVES: {objectives}

WELL INFORMATION: {well_info_text}

FORMATIONS:
{formations_text}

DOCUMENTS: 
{docs_text}

Create a comprehensive drilling plan with the following sections:

## Plan Summary
[Provide overview of drilling strategy]

## BHA Configuration
| Position | Component | Specifications |
|----------|-----------|----------------|
| 1 | [Bit Type] | [Size and specs] |
| 2 | [Motor/Tool] | [Technical details] |

## Drilling Parameters
| Section | Depth Range | WOB | RPM | Flow Rate | Mud Weight |
|---------|-------------|-----|-----|-----------|------------|
| [Section Name] | [Start-End ft] | [klbs] | [rpm] | [gpm] | [ppg] |

## Risk Mitigation
| Risk | Probability | Mitigation Strategy |
|------|-------------|-------------------|
| [Risk Type] | [Low/Med/High] | [Specific approach] |

## Expected Performance
- Estimated Days: [time estimate]
- Target ROP: [ft/hr]
- Cost Estimate: [USD range]

Generate the complete drilling plan now:"""

    # Validate prompt construction
    if not prompt or len(prompt.strip()) < 100:
        raise ValueError("Failed to construct valid prompt")
    
    try:
        result = llm_generate(prompt)
    except Exception as e:
        raise ConnectionError(f"LLM generation failed: {e}")
    
    # STRICT result validation
    if not result:
        raise ValueError("LLM generated empty response")
    
    if len(result.strip()) < 200:
        raise ValueError(f"Generated plan too short: {len(result)} characters")
    
    # Validate required sections are present
    required_sections = ["Plan Summary", "BHA Configuration", "Drilling Parameters"]
    missing_sections = [section for section in required_sections if section not in result]
    
    if missing_sections:
        raise ValueError(f"Generated plan missing required sections: {missing_sections}")
    
    return result


def generate_reflection_markdown(validation_failures: list, current_plan: str, context: dict) -> str:
    """
    Generate reflection and optimization suggestions using markdown format.
    STRICT MODE - No fallbacks, comprehensive validation.
    
    Args:
        validation_failures: List of validation failure descriptions
        current_plan: Current drilling plan text
        context: Well and geological context
        
    Returns:
        Reflection analysis and improvement recommendations in markdown format
        
    Raises:
        ValueError: If inputs are invalid
        ConnectionError: If LLM generation fails
    """
    # STRICT input validation
    if not validation_failures:
        raise ValueError("Validation failures list cannot be empty")
    
    if not isinstance(validation_failures, list):
        raise ValueError(f"Validation failures must be list, got: {type(validation_failures)}")
    
    if not current_plan:
        raise ValueError("Current plan cannot be empty")
    
    if not isinstance(current_plan, str):
        raise ValueError(f"Current plan must be string, got: {type(current_plan)}")
    
    if not context:
        raise ValueError("Context cannot be empty")
    
    if not isinstance(context, dict):
        raise ValueError(f"Context must be dict, got: {type(context)}")
    
    # Validate context has well_info key
    if "well_info" not in context:
        raise KeyError("Context missing required 'well_info' key")
    
    # Format failures for display with validation
    violations_text = ""
    try:
        violations_text = "\n".join([f"- {failure}" for failure in validation_failures if failure])
    except Exception as e:
        raise ValueError(f"Failed to format validation failures: {e}")
    
    if not violations_text:
        raise ValueError("No valid validation failures to process")
    
    # Prepare plan excerpt with validation
    if len(current_plan) < 50:
        raise ValueError("Current plan too short for meaningful reflection")
    
    plan_excerpt = current_plan[:500] + ("..." if len(current_plan) > 500 else "")
    
    # Extract well context with validation
    well_id = context["well_info"].get("well_id", "Unknown well")
    if not isinstance(well_id, str):
        well_id = "Unknown well"
    
    prompt = f"""The drilling plan failed validation. Analyze and provide improvements.

VALIDATION FAILURES:
{violations_text}

CURRENT PLAN EXCERPT:
{plan_excerpt}

CONTEXT: {well_id}

Provide analysis in this format:

## Root Cause Analysis
[Primary technical reason for failure]

## Proposed Change
**Change Type**: [Parameter/Component/Procedure]
**Specific Modification**: [Exact change to implement]
**New Value**: [Recommended value with units]

## Technical Rationale
[Engineering justification for this change]

## Expected Impact
[How this addresses the failure and affects performance]

## Implementation Steps
1. [Specific action]
2. [Specific action]

Provide your analysis:"""

    # Validate prompt construction
    if not prompt or len(prompt.strip()) < 100:
        raise ValueError("Failed to construct valid reflection prompt")
    
    try:
        result = llm_generate(prompt)
    except Exception as e:
        raise ConnectionError(f"Reflection generation failed: {e}")
    
    # STRICT result validation
    if not result:
        raise ValueError("LLM generated empty reflection")
    
    if len(result.strip()) < 100:
        raise ValueError(f"Generated reflection too short: {len(result)} characters")
    
    # Validate required sections are present
    required_sections = ["Root Cause Analysis", "Proposed Change"]
    missing_sections = [section for section in required_sections if section not in result]
    
    if missing_sections:
        raise ValueError(f"Generated reflection missing required sections: {missing_sections}")
    
    return result


def parse_markdown_plan(plan_text: str) -> dict:
    """
    Parse markdown-formatted drilling plan into structured data.
    STRICT MODE - No fallbacks, comprehensive validation.
    
    Args:
        plan_text: Markdown-formatted drilling plan
        
    Returns:
        Dictionary with structured plan data
        
    Raises:
        ValueError: If plan cannot be parsed
    """
    # STRICT input validation
    if not plan_text:
        raise ValueError("Plan text cannot be empty")
    
    if not isinstance(plan_text, str):
        raise ValueError(f"Plan text must be string, got: {type(plan_text)}")
    
    if len(plan_text.strip()) < 50:
        raise ValueError(f"Plan text too short for parsing: {len(plan_text)} characters")
    
    parsed_plan = {
        "plan_text": plan_text,
        "parameters": [],
        "expected_risks": [],
        "bha_configuration": []
    }
    
    try:
        # Extract plan summary
        summary_match = re.search(r'## Plan Summary\n(.*?)(?=##|$)', plan_text, re.DOTALL)
        if summary_match:
            summary = summary_match.group(1).strip()
            if summary:
                parsed_plan["plan_summary"] = summary
        
        # Extract BHA configuration from markdown table
        bha_match = re.search(r'## BHA Configuration\n\|.*?\n\|.*?\n((?:\|.*?\n?)*)', plan_text, re.DOTALL)
        if bha_match:
            bha_rows = bha_match.group(1).strip().split('\n')
            for row in bha_rows:
                if '|' in row and row.strip() != '':
                    parts = [cell.strip() for cell in row.split('|')[1:-1]]
                    if len(parts) >= 3:
                        parsed_plan["bha_configuration"].append({
                            "position": parts[0],
                            "component": parts[1],
                            "specifications": parts[2]
                        })
        
        # Extract drilling parameters
        params_match = re.search(r'## Drilling Parameters\n\|.*?\n\|.*?\n((?:\|.*?\n?)*)', plan_text, re.DOTALL)
        if params_match:
            param_rows = params_match.group(1).strip().split('\n')
            for row in param_rows:
                if '|' in row and row.strip() != '':
                    parts = [cell.strip() for cell in row.split('|')[1:-1]]
                    if len(parts) >= 6:
                        parsed_plan["parameters"].append({
                            "section": parts[0],
                            "depth_range": parts[1],
                            "wob": parts[2],
                            "rpm": parts[3],
                            "flow_rate": parts[4],
                            "mud_weight": parts[5]
                        })
        
        # Extract risk mitigation
        risk_match = re.search(r'## Risk Mitigation\n\|.*?\n\|.*?\n((?:\|.*?\n?)*)', plan_text, re.DOTALL)
        if risk_match:
            risk_rows = risk_match.group(1).strip().split('\n')
            for row in risk_rows:
                if '|' in row and row.strip() != '':
                    parts = [cell.strip() for cell in row.split('|')[1:-1]]
                    if len(parts) >= 3:
                        parsed_plan["expected_risks"].append({
                            "risk_type": parts[0],
                            "probability": parts[1],
                            "mitigation": parts[2]
                        })
        
    except Exception as e:
        parsed_plan["parsing_error"] = str(e)
        raise ValueError(f"Plan parsing failed: {e}")
    
    # Validate parsing results
    if not parsed_plan.get("plan_summary") and not parsed_plan.get("bha_configuration") and not parsed_plan.get("parameters"):
        raise ValueError("Failed to extract any meaningful content from plan")
    
    return parsed_plan


def parse_markdown_reflection(reflection_text: str) -> dict:
    """
    Parse markdown-formatted reflection into structured data.
    STRICT MODE - No fallbacks, comprehensive validation.
    
    Args:
        reflection_text: Markdown-formatted reflection analysis
        
    Returns:
        Dictionary with structured reflection data
        
    Raises:
        ValueError: If reflection cannot be parsed
    """
    # STRICT input validation
    if not reflection_text:
        raise ValueError("Reflection text cannot be empty")
    
    if not isinstance(reflection_text, str):
        raise ValueError(f"Reflection text must be string, got: {type(reflection_text)}")
    
    if len(reflection_text.strip()) < 50:
        raise ValueError(f"Reflection text too short for parsing: {len(reflection_text)} characters")
    
    parsed_reflection = {
        "reflection_text": reflection_text
    }
    
    try:
        # Extract root cause analysis
        root_cause_match = re.search(r'## Root Cause Analysis\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if root_cause_match:
            root_cause = root_cause_match.group(1).strip()
            if root_cause:
                parsed_reflection["root_cause_analysis"] = root_cause
        
        # Extract proposed change details
        change_match = re.search(r'## Proposed Change\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if change_match:
            change_text = change_match.group(1)
            if change_text:
                parsed_reflection["proposed_change"] = change_text.strip()
                
                # Extract specific fields
                change_type_match = re.search(r'\*\*Change Type\*\*: (.*?)(?=\n|\*\*|$)', change_text)
                if change_type_match:
                    change_type = change_type_match.group(1).strip()
                    if change_type:
                        parsed_reflection["change_type"] = change_type
        
        # Extract rationale
        rationale_match = re.search(r'## Technical Rationale\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if rationale_match:
            rationale = rationale_match.group(1).strip()
            if rationale:
                parsed_reflection["rationale"] = rationale
                
    except Exception as e:
        parsed_reflection["parsing_error"] = str(e)
        raise ValueError(f"Reflection parsing failed: {e}")
    
    # Validate parsing results
    if not parsed_reflection.get("root_cause_analysis") and not parsed_reflection.get("proposed_change"):
        raise ValueError("Failed to extract meaningful content from reflection")
    
    return parsed_reflection


def validate_environment() -> None:
    """
    Validate that required WatsonX environment variables are configured.
    STRICT MODE - No fallbacks.
    
    Raises:
        EnvironmentError: If required environment variables are missing
    """
    validate_watsonx_environment()


def get_model_info() -> dict:
    """
    Get information about the current model configuration.
    STRICT MODE - All fields must be present.
    
    Returns:
        Dictionary containing model configuration details
        
    Raises:
        EnvironmentError: If configuration is incomplete
    """
    # Validate environment first
    validate_watsonx_environment()
    
    model_id = os.environ["WX_MODEL_ID"]
    url = os.environ["WX_URL"]
    project_id = os.environ["WX_PROJECT_ID"]
    api_key = os.environ["WX_API_KEY"]
    
    return {
        "model_id": model_id,
        "url": url,
        "project_id": project_id,
        "api_key_configured": bool(api_key),
        "environment_validated": True
    }