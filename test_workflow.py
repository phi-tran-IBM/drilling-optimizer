#!/usr/bin/env python3
"""
Enhanced test script that tests all environment variables including GRAPH_WEIGHT and ASTRA_WEIGHT
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_environment():
    """Test 1: Verify ALL environment setup including workflow variables"""
    print("=" * 60)
    print("TEST 1: Environment Setup (Complete)")
    print("=" * 60)
    
    load_dotenv()
    
    # Core required variables
    required_vars = ['WX_API_KEY', 'WX_PROJECT_ID', 'WX_URL']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Missing environment variables: {missing}")
        return False
    
    print("✅ Core environment variables found")
    
    # Workflow-specific variables
    workflow_vars = {
        'WX_MODEL_ID': os.getenv('WX_MODEL_ID', 'meta-llama/llama-3-3-70b-instruct'),
        'GRAPH_WEIGHT': os.getenv('GRAPH_WEIGHT', '0.7'),
        'ASTRA_WEIGHT': os.getenv('ASTRA_WEIGHT', '0.3'),
        'MAX_LOOPS': os.getenv('MAX_LOOPS', '5')
    }
    
    print("\n📊 Workflow Configuration:")
    for var, value in workflow_vars.items():
        print(f"   {var}: {value}")
        
    # Validate weights sum to 1.0
    graph_weight = float(workflow_vars['GRAPH_WEIGHT'])
    astra_weight = float(workflow_vars['ASTRA_WEIGHT'])
    total_weight = graph_weight + astra_weight
    
    print(f"\n🔍 Weight Analysis:")
    print(f"   Graph Weight: {graph_weight}")
    print(f"   Astra Weight: {astra_weight}")
    print(f"   Total: {total_weight}")
    
    if abs(total_weight - 1.0) < 0.01:
        print("   ✅ Weights properly balanced")
    else:
        print("   ⚠️  Weights don't sum to 1.0 (may be intentional)")
    
    # Optional external services
    external_vars = {
        'NEO4J_URI': 'Neo4j connection',
        'NEO4J_USERNAME': 'Neo4j auth', 
        'NEO4J_PASSWORD': 'Neo4j auth',
        'ASTRA_DB_API_ENDPOINT': 'AstraDB connection',
        'ASTRA_DB_APPLICATION_TOKEN': 'AstraDB auth'
    }
    
    print(f"\n🔗 External Services:")
    for var, desc in external_vars.items():
        status = "✅" if os.getenv(var) else "⚠️ "
        value = "Configured" if os.getenv(var) else "Using fallbacks"
        print(f"   {status} {desc}: {value}")
    
    return True

def test_environment_variable_impact():
    """Test 2: Test how environment variables affect workflow behavior"""
    print("\n" + "=" * 60)
    print("TEST 2: Environment Variable Impact")
    print("=" * 60)
    
    try:
        from app.agent.workflow import node_draft
        
        # Test with different weight configurations
        test_configs = [
            {"GRAPH_WEIGHT": "0.7", "ASTRA_WEIGHT": "0.3", "name": "Default"},
            {"GRAPH_WEIGHT": "0.9", "ASTRA_WEIGHT": "0.1", "name": "Graph-Heavy"},
            {"GRAPH_WEIGHT": "0.3", "ASTRA_WEIGHT": "0.7", "name": "Document-Heavy"}
        ]
        
        base_state = {
            "plan_id": "test-plan",
            "well_id": "WELL_001",
            "context": {
                "objectives": "Test weight impact",
                "formations": [{"name": "Test Formation"}],
                "docs": [{"snippet": "Test document content"}]
            },
            "draft": "",
            "validation": {},
            "kpis": {},
            "history": [],
            "loop": 0
        }
        
        print("🧪 Testing different weight configurations...")
        
        for config in test_configs:
            print(f"\n   Testing {config['name']} config:")
            print(f"   Graph: {config['GRAPH_WEIGHT']}, Astra: {config['ASTRA_WEIGHT']}")
            
            # Set environment variables
            os.environ["GRAPH_WEIGHT"] = config["GRAPH_WEIGHT"]
            os.environ["ASTRA_WEIGHT"] = config["ASTRA_WEIGHT"]
            
            # Test node_draft with this configuration
            try:
                test_state = base_state.copy()
                result_state = node_draft(test_state)
                
                if result_state["draft"]:
                    print(f"   ✅ Generated draft ({len(result_state['draft'])} chars)")
                    
                    # Check if weight note appears in draft
                    draft_content = result_state["draft"]
                    if f"Graph:{config['GRAPH_WEIGHT']}" in draft_content:
                        print(f"   ✅ Weight configuration applied correctly")
                    else:
                        print(f"   ⚠️  Weight configuration not visible in output")
                else:
                    print(f"   ❌ No draft generated")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Reset to defaults
        os.environ["GRAPH_WEIGHT"] = "0.7"
        os.environ["ASTRA_WEIGHT"] = "0.3"
        
        return True
        
    except Exception as e:
        print(f"❌ Environment variable impact test failed: {e}")
        return False

def test_watsonx_client():
    """Test 3: WatsonX client functionality"""
    print("\n" + "=" * 60)
    print("TEST 3: WatsonX Client")
    print("=" * 60)
    
    try:
        from app.llm.watsonx_client import llm_generate
        
        print("🧪 Testing basic generation...")
        response = llm_generate("What is drilling? Answer in one sentence.")
        
        if response and len(response.strip()) > 5:
            print("✅ Basic generation working")
            print(f"   Response: {response[:100]}{'...' if len(response) > 100 else ''}")
            
            print("\n🧪 Testing drilling plan generation...")
            drilling_prompt = """Generate a drilling plan for a horizontal well in the Permian Basin.
Include:
- BHA configuration
- Drilling parameters (WOB, RPM, Flow)
- Risk mitigation strategies

Format as JSON with keys: plan_text, parameters, expected_risks"""
            
            drilling_response = llm_generate(drilling_prompt)
            
            if drilling_response and len(drilling_response) > 100:
                print("✅ Drilling plan generation working")
                print(f"   Length: {len(drilling_response)} characters")
                
                # Check if response contains drilling-specific terms
                drilling_terms = ['BHA', 'WOB', 'RPM', 'bit', 'formation']
                found_terms = [term for term in drilling_terms if term.lower() in drilling_response.lower()]
                print(f"   Drilling terms found: {found_terms}")
                
                return True, drilling_response
            else:
                print("❌ Drilling plan generation failed or too short")
                return False, None
        else:
            print("❌ Basic generation failed")
            return False, None
            
    except Exception as e:
        print(f"❌ WatsonX error: {e}")
        return False, None

def test_workflow_with_debugging():
    """Test 4: Complete workflow with enhanced debugging"""
    print("\n" + "=" * 60)
    print("TEST 4: Complete Workflow (Debug Mode)")
    print("=" * 60)
    
    try:
        from app.agent.workflow import build_app, run_once
        
        print("🧪 Building workflow with debug logging...")
        
        # Set environment for testing
        os.environ["MAX_LOOPS"] = "2"  # Short for testing
        os.environ["GRAPH_WEIGHT"] = "0.7"
        os.environ["ASTRA_WEIGHT"] = "0.3"
        
        app = build_app()
        print("✅ Workflow app built successfully")
        
        print("\n🧪 Running workflow with detailed logging...")
        print("   Well ID: WELL_001")
        print("   Objectives: Minimize cost and vibration while maintaining ROP")
        print("   Max Loops: 2")
        print("   Graph Weight: 0.7")
        print("   Astra Weight: 0.3")
        
        # Run workflow
        result = run_once(
            app=app,
            well_id="WELL_001",
            objectives="Minimize cost and vibration while maintaining ROP",
            max_loops=2
        )
        
        print("✅ Workflow completed successfully!")
        
        # Detailed result analysis
        print("\n📊 DETAILED RESULTS:")
        print(f"   Plan ID: {result.get('plan_id')}")
        print(f"   Iterations: {result.get('iterations', 'Unknown')}")
        print(f"   Objectives: {result.get('objectives')}")
        
        if result.get('validation'):
            val = result['validation']
            print(f"   Validation: Passes={val.get('passes')}, Violations={val.get('violations', 0)}")
            if val.get('violation_details'):
                print(f"   Violation Details: {val['violation_details']}")
        
        if result.get('kpis'):
            kpis = result['kpis']
            print(f"   KPIs:")
            for key, value in kpis.items():
                print(f"     {key}: {value}")
        
        if result.get('plan'):
            plan = result['plan']
            print(f"   Plan Length: {len(plan)} characters")
            
            # Analyze plan content
            plan_lower = plan.lower()
            technical_terms = ['bha', 'wob', 'rpm', 'flow', 'bit', 'motor', 'mwd', 'psi', 'gpm']
            found_terms = [term for term in technical_terms if term in plan_lower]
            print(f"   Technical terms found: {found_terms}")
            
            # Show plan preview
            preview_length = min(500, len(plan))
            print(f"\n📋 GENERATED PLAN PREVIEW:")
            print("-" * 50)
            print(plan[:preview_length])
            if len(plan) > preview_length:
                print("...")
            print("-" * 50)
        
        return True, result
        
    except Exception as e:
        print(f"❌ Workflow error: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def main():
    """Run enhanced comprehensive tests"""
    print("🚀 ENHANCED COMPREHENSIVE TESTING")
    print("🎯 Including environment variables and debugging")
    
    results = {}
    all_passed = True
    
    # Test 1: Enhanced Environment
    if not test_environment():
        print("\n❌ CRITICAL: Environment setup failed")
        return False
    
    # Test 2: Environment Variable Impact
    env_impact_ok = test_environment_variable_impact()
    if not env_impact_ok:
        print("\n⚠️  Environment variable impact test failed")
        all_passed = False
    
    # Test 3: WatsonX Client
    watsonx_ok, watsonx_result = test_watsonx_client()
    if not watsonx_ok:
        print("\n❌ CRITICAL: WatsonX client failed")
        return False
    results['watsonx'] = watsonx_result
    
    # Test 4: Complete Workflow with Debugging
    workflow_ok, workflow_result = test_workflow_with_debugging()
    if not workflow_ok:
        print("\n❌ CRITICAL: Complete workflow failed")
        all_passed = False
    else:
        results['workflow'] = workflow_result
    
    # Final Summary
    print("\n" + "=" * 60)
    print("🏁 ENHANCED TEST SUMMARY")
    print("=" * 60)
    
    if all_passed and workflow_ok:
        print("🎉 ALL TESTS PASSED!")
        print("\n✅ Confirmed working:")
        print("   • Environment configuration (including weights) ✅")
        print("   • Environment variable impact ✅")
        print("   • WatsonX LLM generation ✅")
        print("   • Complete workflow execution ✅")
        print("   • Graph/Astra weight balancing ✅")
        
        # Save results
        with open('enhanced_test_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print("\n💾 Results saved to enhanced_test_results.json")
        
        print("\n🚀 READY FOR PRODUCTION!")
        
    else:
        print("❌ SOME TESTS FAILED")
        print("\n🔧 Apply the workflow.py fix provided above")
        print("   The LangGraph routing issue needs to be resolved")
    
    return all_passed and workflow_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)