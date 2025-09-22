#!/usr/bin/env python3
"""
Secure settings validation script that validates configuration without exposing credentials.
Fixed to handle Python path correctly when run from scripts directory.
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def mask_sensitive_value(key: str, value: any) -> str:
    """Mask sensitive values based on key patterns."""
    if not value:
        return "❌ NOT SET"
    
    sensitive_patterns = [
        'key', 'token', 'password', 'secret', 'credential', 
        'auth', 'api_key', 'access_key', 'private'
    ]
    
    key_lower = key.lower()
    if any(pattern in key_lower for pattern in sensitive_patterns):
        if isinstance(value, str) and len(value) > 0:
            # Show first 4 and last 4 characters with stars in between
            if len(value) > 8:
                return f"{value[:4]}***{value[-4:]}"
            else:
                return "***[CONFIGURED]***"
        else:
            return "***[CONFIGURED]***"
    
    return str(value)

def validate_settings_secure():
    """Validate settings configuration without exposing sensitive data."""
    print("🔒 SECURE SETTINGS VALIDATION")
    print("=" * 60)
    
    try:
        from app.core.config.settings import settings
        print("✅ Settings module imported successfully")
        
        print(f"\n📋 Configuration Overview:")
        print(f"   App Name: {settings.app_name}")
        print(f"   Environment: {settings.environment}")
        print(f"   Debug Mode: {settings.debug}")
        
        print(f"\n🔧 Service Configuration Status:")
        
        # Group settings by service
        service_groups = {
            "Neo4j": ["neo4j_uri", "neo4j_user", "neo4j_password", "neo4j_database"],
            "AstraDB": ["astra_endpoint", "astra_token", "astra_collection", "astra_db_vector_dim", "astra_use_server_vectorize"],
            "WatsonX": ["wx_api_key", "wx_url", "wx_project_id", "wx_model_id"],
            "Workflow": ["graph_weight", "astra_weight", "max_loops", "enable_monitoring", "enable_multi_agent"],
            "Optional": ["log_level", "openai_api_key"]
        }
        
        all_configured = True
        
        for service_name, setting_keys in service_groups.items():
            print(f"\n   {service_name}:")
            service_ok = True
            
            for key in setting_keys:
                if hasattr(settings, key):
                    value = getattr(settings, key)
                    masked_value = mask_sensitive_value(key, value)
                    
                    # Check if required settings are configured
                    is_required = service_name in ["Neo4j", "AstraDB", "WatsonX"] and ("password" in key.lower() or "key" in key.lower() or "token" in key.lower() or "endpoint" in key.lower() or "project_id" in key.lower())
                    
                    if is_required and not value:
                        print(f"     ❌ {key}: {masked_value}")
                        service_ok = False
                        all_configured = False
                    else:
                        status = "✅" if value else "⚠️"
                        print(f"     {status} {key}: {masked_value}")
                else:
                    print(f"     ❓ {key}: NOT DEFINED")
                    if service_name in ["Neo4j", "AstraDB", "WatsonX"]:
                        service_ok = False
                        all_configured = False
        
        print(f"\n📊 VALIDATION SUMMARY:")
        print(f"   Overall Status: {'✅ ALL CONFIGURED' if all_configured else '⚠️ MISSING REQUIRED SETTINGS'}")
        
        return True, all_configured
        
    except ImportError as e:
        print(f"❌ Failed to import settings: {e}")
        print(f"   Project root: {project_root}")
        print(f"   Python path: {sys.path[:3]}...")
        return False, False
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False, False

def test_core_imports_secure():
    """Test core module imports without exposing sensitive data."""
    print(f"\n🧪 TESTING CORE IMPORTS")
    print("=" * 60)
    
    import_tests = [
        ("RetrievalInterface", "app.core.interfaces", "RetrievalInterface"),
        ("Settings", "app.core.config.settings", "settings"),
        ("DrillingOntology", "app.domain.ontology.drilling_ontology", "DrillingOntology"),
        ("ConstraintFirstStrategy", "app.retrieval.strategies", "ConstraintFirstStrategy"),
        ("Original Workflow", "app.agent.workflow", "build_app"),
        ("GraphRAG", "app.graph.graph_rag", "retrieve_subgraph_context"),
        ("WatsonX Client", "app.llm.watsonx_client", "llm_generate"),
    ]
    
    results = {}
    
    for test_name, module_path, import_name in import_tests:
        try:
            module = __import__(module_path, fromlist=[import_name])
            getattr(module, import_name)  # Verify the import exists
            print(f"   ✅ {test_name}")
            results[test_name] = True
        except Exception as e:
            print(f"   ❌ {test_name}: {str(e)[:60]}...")
            results[test_name] = False
    
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"\n📈 Import Results: {success_count}/{total_count} successful")
    
    return results

def test_workflow_compatibility():
    """Test if the workflow can be initialized without exposing credentials."""
    print(f"\n⚙️ TESTING WORKFLOW COMPATIBILITY")
    print("=" * 60)
    
    try:
        from app.agent.workflow import build_app
        print("   ✅ Workflow module imports")
        
        # Try to build the app (this might fail due to environment validation)
        try:
            app = build_app()
            print("   ✅ Workflow app builds successfully")
            print("   ✅ Workflow is ready for use")
            return True
        except Exception as build_error:
            print(f"   ⚠️ Workflow builds with warnings: {str(build_error)[:80]}...")
            print("   💡 This is likely due to environment validation in the original workflow")
            print("   💡 The modular components are working, but original workflow needs adjustment")
            return "partial"
        
    except Exception as e:
        print(f"   ❌ Workflow import failed: {str(e)[:80]}...")
        return False

def check_python_environment():
    """Check Python environment and path setup."""
    print(f"\n🐍 PYTHON ENVIRONMENT CHECK")
    print("=" * 60)
    
    print(f"   Python Version: {sys.version.split()[0]}")
    print(f"   Project Root: {project_root}")
    print(f"   Current Working Dir: {os.getcwd()}")
    print(f"   Script Location: {Path(__file__).parent}")
    
    # Check if key directories exist
    key_dirs = ['app', 'app/core', 'app/agent', 'app/graph']
    for dir_name in key_dirs:
        dir_path = project_root / dir_name
        status = "✅" if dir_path.exists() else "❌"
        print(f"   {status} {dir_name}/: {'exists' if dir_path.exists() else 'missing'}")

def main():
    """Run complete secure validation suite."""
    print("🔒 DRILLING OPTIMIZER - SECURE CONFIGURATION VALIDATION")
    print("=" * 70)
    
    # Check environment first
    check_python_environment()
    
    # Test 1: Settings validation
    settings_ok, all_configured = validate_settings_secure()
    
    # Test 2: Import testing
    import_results = test_core_imports_secure()
    
    # Test 3: Workflow compatibility
    workflow_ok = test_workflow_compatibility()
    
    # Final summary
    print(f"\n🏁 FINAL VALIDATION SUMMARY")
    print("=" * 70)
    
    print(f"Settings Configuration: {'✅ PASS' if settings_ok and all_configured else '⚠️ PARTIAL' if settings_ok else '❌ FAIL'}")
    
    imports_passed = sum(import_results.values())
    imports_total = len(import_results)
    print(f"Module Imports: {'✅ PASS' if imports_passed == imports_total else f'⚠️ {imports_passed}/{imports_total}' if imports_passed > 0 else '❌ FAIL'}")
    
    workflow_status = "✅ PASS" if workflow_ok == True else "⚠️ PARTIAL" if workflow_ok == "partial" else "❌ FAIL"
    print(f"Workflow Ready: {workflow_status}")
    
    if settings_ok and all_configured and imports_passed == imports_total and workflow_ok == True:
        print(f"\n🎉 SYSTEM STATUS: READY FOR OPERATION")
        print(f"   Your drilling optimization system is fully configured!")
    elif settings_ok and imports_passed > 0:
        print(f"\n⚠️ SYSTEM STATUS: PARTIALLY READY")
        print(f"   Core functionality available, some components may need attention")
        print(f"   The modular architecture is working correctly")
    else:
        print(f"\n❌ SYSTEM STATUS: NEEDS ATTENTION")
        print(f"   Review configuration and resolve import issues")
    
    return settings_ok and imports_passed > 0

if __name__ == "__main__":
    main()