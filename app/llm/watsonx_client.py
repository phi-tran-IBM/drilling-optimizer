import os
from dotenv import load_dotenv
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams
import re

load_dotenv()

def llm_generate(prompt: str) -> str:
    """
    Generate text using IBM WatsonX foundation models for drilling engineering tasks.
    
    Args:
        prompt: Input prompt for text generation
        
    Returns:
        Generated text response
        
    Raises:
        Exception: If generation fails or credentials are missing
    """
    model_id = os.getenv("WX_MODEL_ID", "meta-llama/llama-3-3-70b-instruct")
    api_key = os.getenv("WX_API_KEY")
    url = os.getenv("WX_URL", "https://us-south.ml.cloud.ibm.com")
    project_id = os.getenv("WX_PROJECT_ID")
    
    if not api_key:
        raise ValueError("WX_API_KEY environment variable is required")
    if not project_id:
        raise ValueError("WX_PROJECT_ID environment variable is required")
    
    params = {
        GenParams.MAX_NEW_TOKENS: 3000,
        GenParams.TEMPERATURE: 0.5,
        GenParams.DECODING_METHOD: "greedy",
        GenParams.REPETITION_PENALTY: 1.05,
        GenParams.TOP_P: 0.9,
        GenParams.TOP_K: 50
    }
    
    model = ModelInference(
        model_id=model_id,
        params=params,
        credentials={"url": url, "apikey": api_key},
        project_id=project_id
    )
    
    response = model.generate_text(prompt=prompt)
    
    if isinstance(response, str):
        generated_text = response
    elif isinstance(response, dict):
        generated_text = response.get("results", [{}])[0].get("generated_text", "")
    else:
        generated_text = str(response)
    
    # Content validation for drilling engineering context
    if any(unwanted in generated_text.lower() for unwanted in ["#include", "void main", "<stdio.h>", "function main"]):
        raise ValueError("Model generated programming code instead of drilling engineering content")
    
    # Remove the strict length check that was causing issues
    if len(generated_text.strip()) < 3:
        raise ValueError("Generated response is too short")
    
    return generated_text.strip()


def generate_drilling_plan_markdown(context: dict, objectives: str, graph_weight: float = 0.7, astra_weight: float = 0.3) -> str:
    """
    Generate a comprehensive drilling plan using markdown formatting optimized for LLM generation.
    
    This function creates drilling plans with markdown output format for better LLM reliability.
    
    Args:
        context: Well and geological context data from GraphRAG
        objectives: Planning objectives and constraints
        graph_weight: Weight for graph-based evidence
        astra_weight: Weight for document-based evidence
        
    Returns:
        Generated drilling plan in markdown format
    """
    # Extract context components
    doc_bits = context.get("docs", [])
    well_info = context.get("well_info", {})
    formations = context.get("formations", [])
    offset_wells = context.get("offset_wells", [])
    historical_performance = context.get("historical_performance", {})
    
    # Build weight annotation for transparency
    weight_note = f"Evidence weights → Graph:{graph_weight:.2f} Astra:{astra_weight:.2f}"
    
    # Format context information
    well_info_text = f"Well ID: {well_info.get('well_id', 'Unknown')}, Location: {well_info.get('location', 'Unknown')}"
    
    formations_text = ""
    if formations:
        formations_text = "\n".join([
            f"- {f.get('name', 'Unknown')}: {f.get('depth', [0, 0])[0]}-{f.get('depth', [0, 0])[1]} ft"
            for f in formations[:3]
        ])
    else:
        formations_text = "No formation data available"
    
    # Enhanced prompt with simplified structure for better generation
    prompt = f"""You are an expert drilling engineer. Create a detailed drilling plan.

{weight_note}

OBJECTIVES: {objectives}

WELL INFORMATION: {well_info_text}

FORMATIONS:
{formations_text}

DOCUMENTS: {doc_bits[:2] if doc_bits else ['No relevant documents']}

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

    return llm_generate(prompt)


def generate_reflection_markdown(validation_failures: list, current_plan: str, context: dict) -> str:
    """
    Generate reflection and optimization suggestions using markdown format.
    
    Args:
        validation_failures: List of validation failure descriptions
        current_plan: Current drilling plan text
        context: Well and geological context
        
    Returns:
        Reflection analysis and improvement recommendations in markdown format
    """
    # Format failures for display
    violations_text = "\n".join([f"- {failure}" for failure in validation_failures])
    plan_excerpt = current_plan[:500] + ("..." if len(current_plan) > 500 else "")
    
    prompt = f"""The drilling plan failed validation. Analyze and provide improvements.

VALIDATION FAILURES:
{violations_text}

CURRENT PLAN EXCERPT:
{plan_excerpt}

CONTEXT: {context.get('well_info', {}).get('well_id', 'Unknown well')}

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

    return llm_generate(prompt)


def parse_markdown_plan(plan_text: str) -> dict:
    """
    Parse markdown-formatted drilling plan into structured data.
    
    Args:
        plan_text: Markdown-formatted drilling plan
        
    Returns:
        Dictionary with structured plan data
    """
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
            parsed_plan["plan_summary"] = summary_match.group(1).strip()
        
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
    
    return parsed_plan


def parse_markdown_reflection(reflection_text: str) -> dict:
    """
    Parse markdown-formatted reflection into structured data.
    
    Args:
        reflection_text: Markdown-formatted reflection analysis
        
    Returns:
        Dictionary with structured reflection data
    """
    parsed_reflection = {
        "reflection_text": reflection_text
    }
    
    try:
        # Extract root cause analysis
        root_cause_match = re.search(r'## Root Cause Analysis\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if root_cause_match:
            parsed_reflection["root_cause_analysis"] = root_cause_match.group(1).strip()
        
        # Extract proposed change details
        change_match = re.search(r'## Proposed Change\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if change_match:
            change_text = change_match.group(1)
            parsed_reflection["proposed_change"] = change_text.strip()
            
            # Extract specific fields
            change_type_match = re.search(r'\*\*Change Type\*\*: (.*?)(?=\n|\*\*|$)', change_text)
            if change_type_match:
                parsed_reflection["change_type"] = change_type_match.group(1).strip()
        
        # Extract rationale
        rationale_match = re.search(r'## Technical Rationale\n(.*?)(?=##|$)', reflection_text, re.DOTALL)
        if rationale_match:
            parsed_reflection["rationale"] = rationale_match.group(1).strip()
            
    except Exception as e:
        parsed_reflection["parsing_error"] = str(e)
    
    return parsed_reflection


def validate_environment() -> None:
    """
    Validate that required WatsonX environment variables are configured.
    
    Raises:
        ValueError: If required environment variables are missing
    """
    required_vars = ["WX_API_KEY", "WX_PROJECT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")


def get_model_info() -> dict:
    """
    Get information about the current model configuration.
    
    Returns:
        Dictionary containing model configuration details
    """
    return {
        "model_id": os.getenv("WX_MODEL_ID", "meta-llama/llama-3-3-70b-instruct"),
        "url": os.getenv("WX_URL", "https://us-south.ml.cloud.ibm.com"),
        "project_id": os.getenv("WX_PROJECT_ID"),
        "api_key_configured": bool(os.getenv("WX_API_KEY"))
    }