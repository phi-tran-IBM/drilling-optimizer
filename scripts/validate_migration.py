#!/usr/bin/env python3
"""
Validate that the migration was successful and everything is working
"""
import os
import sys
from pathlib import Path
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def validate_directory_structure():
    """Check that all directories exist"""
    print("\n📁 Validating directory structure...")
    
    required_dirs = [
        "app/core/interfaces",
        "app/core/config",
        "app/domain/models",
        "app/domain/ontology",
        "app/retrieval/strategies",
        "app/ingest/extractors",
        "config/environments",
        "data/demo/permian"
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        if full_path.exists():
            print(f"   ✅ {dir_path}")
        else:
            print(f"   ❌ {dir_path} - MISSING")
            all_exist = False
    
    return all_exist

def validate_core_files():
    """Check that core files were created"""
    print("\n📄 Validating core files...")
    
    required_files = [
        "app/core/interfaces/retrieval_interface.py",
        "app/core/interfaces/knowledge_graph_interface.py",
        "app/core/config/settings.py",
        "app/domain/models/drilling_plan.py",
        "app/domain/ontology/drilling_ontology.py",
        "app/retrieval/strategies/base_strategy.py",
        "app/retrieval/strategies/constraint_first_strategy.py",
        "config/environments/development.json",
        "scripts/migrate_to_modular.py"
    ]
    
    all_exist = True
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} - MISSING")
            all_exist = False
    
    return all_exist

def validate_backwards_compatibility():
    """Check that existing functionality still works"""
    print("\n🔄 Validating backward compatibility...")
    
    try:
        # Test that old imports still work
        from app.graph.graph_rag import retrieve_subgraph_context
        print("   ✅ Original graph_rag imports work")
        
        from app.agent.workflow import build_app
        print("   ✅ Original workflow imports work")
        
        from app.llm.watsonx_client import llm_generate
        print("   ✅ Original LLM client imports work")
        
        return True
        
    except ImportError as e:
        print(f"   ❌ Import error: {e}")
        return False

def validate_new_features():
    """Check that new modular features work"""
    print("\n✨ Validating new features...")
    
    try:
        # Test settings
        from app.core.config.settings import settings
        print(f"   ✅ Settings loaded: env={settings.environment}")
        
        # Test interfaces
        from app.core.interfaces import RetrievalInterface
        print("   ✅ Interfaces accessible")
        
        # Test domain models
        from app.domain.models.drilling_plan import DrillingPlan
        print("   ✅ Domain models accessible")
        
        # Test ontology
        from app.domain.ontology.drilling_ontology import DrillingOntology
        print("   ✅ Ontology accessible")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def main():
    print("=" * 60)
    print("🔍 MIGRATION VALIDATION")
    print("=" * 60)
    
    results = {
        "directory_structure": validate_directory_structure(),
        "core_files": validate_core_files(),
        "backwards_compatibility": validate_backwards_compatibility(),
        "new_features": validate_new_features()
    }
    
    print("\n" + "=" * 60)
    print("📊 VALIDATION RESULTS")
    print("=" * 60)
    
    for check, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{check:.<30} {status}")
    
    if all(results.values()):
        print("\n🎉 All validations passed! Your migration is complete.")
        print("\n📝 Next steps:")
        print("1. Test the API: python -m uvicorn app.main:app --reload")
        print("2. Try the new endpoint: POST /plan/run/v2")
        print("3. Run tests: python -m pytest app/tests/")
    else:
        print("\n⚠️  Some validations failed. Run the migration script again:")
        print("   python scripts/migrate_to_modular.py")

if __name__ == "__main__":
    main()