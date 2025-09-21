import sys
import os
try:
    import neo4j
    print("✅ Neo4j import successful in VS Code!")
    print('Python executable:', sys.executable)
    print('Virtual env:', os.environ.get('VIRTUAL_ENV', 'Not set'))
    print('Neo4j location:', neo4j.__file__)
except ImportError as e:
    print("❌ Neo4j import failed in VS Code:", e)
    print('Python executable:', sys.executable)
    print('Virtual env:', os.environ.get('VIRTUAL_ENV', 'Not set'))