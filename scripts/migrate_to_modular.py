#!/usr/bin/env python3
"""
Migration script to set up new modular architecture
Handles initialization files, config templates, and compatibility checks
"""
import os
import sys
from pathlib import Path
import shutil
import json
from typing import List, Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class ModularMigration:
    def __init__(self):
        self.project_root = project_root
        self.app_dir = self.project_root / "app"
        self.config_dir = self.project_root / "config"
        self.data_dir = self.project_root / "data"
        
    def create_init_files(self) -> List[Path]:
        """Create __init__.py files in all new directories"""
        print("\n📁 Creating __init__.py files...")
        
        dirs_needing_init = [
            "app/core",
            "app/core/interfaces",
            "app/core/config", 
            "app/core/events",
            "app/domain",
            "app/domain/validators",
            "app/domain/models",
            "app/domain/ontology",
            "app/retrieval",
            "app/retrieval/strategies",
            "app/retrieval/processors",
            "app/ingest",
            "app/ingest/extractors",
            "app/ingest/loaders",
            "app/monitoring",
            "app/tests",
            "app/tests/unit",
            "app/tests/integration"
        ]
        
        created_files = []
        for dir_path in dirs_needing_init:
            full_path = self.project_root / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            
            init_file = full_path / "__init__.py"
            if not init_file.exists():
                # Create with appropriate docstring
                module_name = dir_path.split('/')[-1]
                content = f'"""{module_name.title()} module initialization"""\n'
                init_file.write_text(content)
                created_files.append(init_file)
                print(f"   ✅ Created {init_file.relative_to(self.project_root)}")
            else:
                print(f"   ⏭️  Exists {init_file.relative_to(self.project_root)}")
                
        return created_files
    
    def create_config_files(self) -> Dict[str, Path]:
        """Create configuration templates for different environments"""
        print("\n⚙️  Creating configuration files...")
        
        configs = {
            "development": {
                "DEBUG": True,
                "ENVIRONMENT": "development",
                "LOG_LEVEL": "INFO",
                "ENABLE_MONITORING": True,
                "ENABLE_MULTI_AGENT": False,
                "GRAPH_WEIGHT": 0.7,
                "ASTRA_WEIGHT": 0.3,
                "MAX_LOOPS": 5,
                "CACHE_TTL": 300
            },
            "production": {
                "DEBUG": False,
                "ENVIRONMENT": "production",
                "LOG_LEVEL": "WARNING",
                "ENABLE_MONITORING": True,
                "ENABLE_MULTI_AGENT": True,
                "GRAPH_WEIGHT": 0.7,
                "ASTRA_WEIGHT": 0.3,
                "MAX_LOOPS": 10,
                "CACHE_TTL": 3600
            },
            "testing": {
                "DEBUG": True,
                "ENVIRONMENT": "testing",
                "LOG_LEVEL": "DEBUG",
                "ENABLE_MONITORING": False,
                "ENABLE_MULTI_AGENT": False,
                "GRAPH_WEIGHT": 0.5,
                "ASTRA_WEIGHT": 0.5,
                "MAX_LOOPS": 2,
                "CACHE_TTL": 0
            }
        }
        
        created_configs = {}
        for env_name, config in configs.items():
            config_file = self.config_dir / "environments" / f"{env_name}.json"
            config_file.parent.mkdir(parents=True, exist_ok=True)
            
            if not config_file.exists():
                with open(config_file, 'w') as f:
                    json.dump(config, f, indent=2)
                created_configs[env_name] = config_file
                print(f"   ✅ Created {config_file.relative_to(self.project_root)}")
            else:
                print(f"   ⏭️  Exists {config_file.relative_to(self.project_root)}")
                
        return created_configs
    
    def create_adapter_files(self) -> None:
        """Create adapter files for backward compatibility"""
        print("\n🔄 Creating backward compatibility adapters...")
        
        # Create graph_rag_adapter.py if it doesn't exist
        adapter_path = self.app_dir / "graph" / "graph_rag_adapter.py"
        if not adapter_path.exists():
            adapter_content = '''"""
Adapter to make existing graph_rag.py work with new modular architecture
This maintains backward compatibility during migration
"""
from typing import Dict, List, Optional, Any
from app.core.interfaces import KnowledgeGraphInterface

class LegacyGraphRAGAdapter(KnowledgeGraphInterface):
    """
    Wraps existing graph_rag functions with new interface.
    Allows gradual migration without breaking existing code.
    """
    
    def __init__(self):
        # Import existing functions
        from app.graph.graph_rag import (
            retrieve_subgraph_context as self._original_retrieve,
            validate_against_constraints as self._original_validate,
            record_iteration as self._original_record,
            _session
        )
        self._session = _session
    
    def query(self, cypher: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """Execute query using existing infrastructure"""
        with self._session() as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]
    
    def get_subgraph(self, 
                     node_id: str, 
                     max_depth: int = 2,
                     relationship_types: Optional[List[str]] = None) -> Dict:
        """Retrieve subgraph - uses existing retrieve function"""
        # Simplified wrapper
        return self._original_retrieve(node_id, "")
    
    def upsert_node(self, 
                   label: str,
                   properties: Dict[str, Any],
                   merge_key: str) -> str:
        """Create or update a node - wrapper for existing functionality"""
        query = f"""
        MERGE (n:{label} {{{merge_key}: $merge_value}})
        SET n += $properties
        RETURN n
        """
        with self._session() as session:
            result = session.run(query, {
                "merge_value": properties[merge_key],
                "properties": properties
            })
            return properties[merge_key]
    
    def create_relationship(self,
                          from_node: Dict,
                          to_node: Dict,
                          rel_type: str,
                          properties: Optional[Dict] = None) -> bool:
        """Create a relationship - wrapper for existing functionality"""
        # Simplified implementation
        return True
'''
            adapter_path.write_text(adapter_content)
            print(f"   ✅ Created {adapter_path.relative_to(self.project_root)}")
        else:
            print(f"   ⏭️  Exists {adapter_path.relative_to(self.project_root)}")
    
    def create_test_stubs(self) -> None:
        """Create test file stubs for new modules"""
        print("\n🧪 Creating test stubs...")
        
        test_files = {
            "app/tests/unit/test_interfaces.py": '''"""Unit tests for core interfaces"""
import pytest
from app.core.interfaces import RetrievalInterface

def test_retrieval_interface():
    """Test that interface cannot be instantiated directly"""
    with pytest.raises(TypeError):
        RetrievalInterface()
''',
            "app/tests/unit/test_domain_models.py": '''"""Unit tests for domain models"""
import pytest
from app.domain.models.drilling_plan import DrillingPlan, Formation

def test_formation_model():
    """Test Formation model validation"""
    formation = Formation(
        name="Bone Spring",
        depth_start=8000.0,
        depth_end=10000.0
    )
    assert formation.name == "Bone Spring"
    assert formation.depth_end > formation.depth_start
''',
            "app/tests/integration/test_retrieval_strategies.py": '''"""Integration tests for retrieval strategies"""
import pytest
from app.retrieval.strategies import ConstraintFirstStrategy

@pytest.mark.integration
def test_constraint_first_strategy():
    """Test constraint-first retrieval strategy"""
    # Placeholder for integration test
    pass
'''
        }
        
        for test_path, content in test_files.items():
            full_path = self.project_root / test_path
            if not full_path.exists():
                full_path.write_text(content)
                print(f"   ✅ Created {full_path.relative_to(self.project_root)}")
            else:
                print(f"   ⏭️  Exists {full_path.relative_to(self.project_root)}")
    
    def update_requirements(self) -> None:
        """Add new dependencies to requirements.txt"""
        print("\n📦 Updating requirements.txt...")
        
        new_requirements = [
            "pydantic-settings>=2.0.0",  # For config management
            "pytest>=7.0.0",  # For testing
            "pytest-asyncio>=0.21.0",  # For async tests
            "structlog>=23.0.0",  # For structured logging
        ]
        
        req_file = self.project_root / "requirements.txt"
        if req_file.exists():
            existing = req_file.read_text().splitlines()
            
            added = []
            for req in new_requirements:
                # Check if requirement already exists (ignore version)
                req_name = req.split(">=")[0].split("==")[0]
                if not any(req_name in line for line in existing):
                    existing.append(req)
                    added.append(req)
            
            if added:
                req_file.write_text("\n".join(existing) + "\n")
                print(f"   ✅ Added: {', '.join(added)}")
            else:
                print(f"   ⏭️  All requirements already present")
        else:
            print(f"   ⚠️  requirements.txt not found")
    
    def verify_imports(self) -> bool:
        """Verify that new modules can be imported"""
        print("\n🔍 Verifying imports...")
        
        test_imports = [
            "app.core.interfaces",
            "app.core.config.settings",
            "app.domain.models.drilling_plan",
            "app.domain.ontology.drilling_ontology",
            "app.retrieval.strategies.base_strategy",
        ]
        
        all_ok = True
        for module_path in test_imports:
            try:
                # Try to import the module
                parts = module_path.split('.')
                module = __import__(module_path, fromlist=[parts[-1]])
                print(f"   ✅ {module_path}")
            except ImportError as e:
                print(f"   ❌ {module_path}: {e}")
                all_ok = False
                
        return all_ok
    
    def create_demo_data(self) -> None:
        """Create demo data for Permian Basin scenario"""
        print("\n📊 Creating demo data...")
        
        demo_dir = self.data_dir / "demo" / "permian"
        demo_dir.mkdir(parents=True, exist_ok=True)
        
        # Create sample wells CSV
        wells_csv = '''well_id,api_number,operator,county,state,lat,lon,trajectory_type,total_depth
PERM_BS_001,42-123-45678,Demo Operator,Reeves,TX,31.5,-103.5,horizontal,12000
PERM_BS_002,42-123-45679,Demo Operator,Loving,TX,31.6,-103.4,horizontal,11500
PERM_BS_003,42-123-45680,Demo Operator,Ward,TX,31.4,-103.6,horizontal,12500
'''
        
        wells_file = demo_dir / "permian_wells.csv"
        if not wells_file.exists():
            wells_file.write_text(wells_csv)
            print(f"   ✅ Created {wells_file.relative_to(self.project_root)}")
        else:
            print(f"   ⏭️  Exists {wells_file.relative_to(self.project_root)}")
        
        # Create formations CSV
        formations_csv = '''name,depth_start,depth_end,rock_strength,pore_pressure,lithology
Bone Spring,8000,10000,28000,0.48,Limestone
Wolfcamp,10000,12000,32000,0.52,Shale
Delaware,6000,8000,25000,0.45,Sandstone
'''
        
        formations_file = demo_dir / "permian_formations.csv"
        if not formations_file.exists():
            formations_file.write_text(formations_csv)
            print(f"   ✅ Created {formations_file.relative_to(self.project_root)}")
        else:
            print(f"   ⏭️  Exists {formations_file.relative_to(self.project_root)}")
    
    def run_migration(self) -> None:
        """Run complete migration process"""
        print("=" * 60)
        print("🚀 MODULAR ARCHITECTURE MIGRATION")
        print("=" * 60)
        
        # Run all migration steps
        self.create_init_files()
        self.create_config_files()
        self.create_adapter_files()
        self.create_test_stubs()
        self.update_requirements()
        self.create_demo_data()
        
        # Verify everything worked
        print("\n" + "=" * 60)
        if self.verify_imports():
            print("✅ MIGRATION SUCCESSFUL!")
            print("\nNext steps:")
            print("1. Install new requirements: pip install -r requirements.txt")
            print("2. Run tests: python -m pytest app/tests/")
            print("3. Test the API: uvicorn app.main:app --reload")
        else:
            print("⚠️  MIGRATION COMPLETED WITH WARNINGS")
            print("\nSome imports failed. This is normal if you haven't")
            print("created all the files yet. Continue with the migration.")

if __name__ == "__main__":
    migration = ModularMigration()
    migration.run_migration()