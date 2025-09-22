#!/usr/bin/env python3
"""
Test individual service connections to debug the issues
"""

def test_neo4j_connection():
    """Test Neo4j connection with current settings"""
    try:
        from app.core.config.settings import settings
        from neo4j import GraphDatabase
        
        print("🧪 Testing Neo4j Connection...")
        print(f"   URI: {settings.neo4j_uri}")
        print(f"   User: {settings.neo4j_user}")
        print(f"   Password: {'*' * len(settings.neo4j_password) if settings.neo4j_password else 'NOT SET'}")
        
        driver = GraphDatabase.driver(
            settings.neo4j_uri, 
            auth=(settings.neo4j_user, settings.neo4j_password)
        )
        
        with driver.session() as session:
            result = session.run("RETURN 'Hello Neo4j!' as message")
            record = result.single()
            print(f"   ✅ Neo4j Connected: {record['message']}")
            
        driver.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Neo4j Connection Failed: {e}")
        print(f"   💡 Try: docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/{settings.neo4j_password} neo4j:5.16.0")
        return False

def test_astra_connection():
    """Test AstraDB connection"""
    try:
        from app.core.config.settings import settings
        from astrapy import DataAPIClient
        
        print("🧪 Testing AstraDB Connection...")
        print(f"   Endpoint: {settings.astra_endpoint}")
        print(f"   Token: {settings.astra_token[:10]}***")
        
        client = DataAPIClient()
        db = client.get_database(
            settings.astra_endpoint, 
            token=settings.astra_token,
            keyspace="well_planning"
        )
        
        # List collections to test connection
        collections = db.list_collection_names()
        print(f"   ✅ AstraDB Connected. Collections: {collections}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ AstraDB Connection Failed: {e}")
        return False

def test_watsonx_connection():
    """Test WatsonX connection"""
    try:
        from app.llm.watsonx_client import llm_generate
        
        print("🧪 Testing WatsonX Connection...")
        
        response = llm_generate("Say 'Hello from WatsonX!' in one sentence.")
        
        if response and len(response.strip()) > 5:
            print(f"   ✅ WatsonX Connected: {response[:50]}...")
            return True
        else:
            print(f"   ❌ WatsonX returned empty response")
            return False
            
    except Exception as e:
        print(f"   ❌ WatsonX Connection Failed: {e}")
        return False

def create_astra_collection():
    """Create the AstraDB collection if it doesn't exist"""
    try:
        from app.core.config.settings import settings
        from astrapy import DataAPIClient
        
        print("🔧 Creating AstraDB Collection...")
        
        client = DataAPIClient()
        db = client.get_database(
            settings.astra_endpoint, 
            token=settings.astra_token,
            keyspace="well_planning"
        )
        
        collection_name = settings.astra_collection
        
        if collection_name in db.list_collection_names():
            print(f"   ✅ Collection '{collection_name}' already exists")
            return True
        
        # Try to create with vectorize first
        try:
            db.create_collection(
                collection_name,
                dimension=1536,
                metric="cosine",
                service={
                    "provider": "openai",
                    "modelName": "text-embedding-3-small"
                }
            )
            print(f"   ✅ Created collection '{collection_name}' with vectorize")
        except Exception:
            # Fallback to basic collection
            db.create_collection(collection_name, dimension=1536, metric="cosine")
            print(f"   ✅ Created basic collection '{collection_name}'")
            
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to create AstraDB collection: {e}")
        return False

def main():
    """Test all connections and setup"""
    print("🔍 TESTING SERVICE CONNECTIONS")
    print("=" * 50)
    
    # Test connections
    neo4j_ok = test_neo4j_connection()
    astra_ok = test_astra_connection()
    watsonx_ok = test_watsonx_connection()
    
    print("\n🔧 SETUP FIXES")
    print("=" * 50)
    
    # Create AstraDB collection if connection works but collection missing
    if astra_ok:
        create_astra_collection()
    
    print("\n📊 SUMMARY")
    print("=" * 50)
    print(f"Neo4j: {'✅ Ready' if neo4j_ok else '❌ Needs Fix'}")
    print(f"AstraDB: {'✅ Ready' if astra_ok else '❌ Needs Fix'}")
    print(f"WatsonX: {'✅ Ready' if watsonx_ok else '❌ Needs Fix'}")
    
    if neo4j_ok and astra_ok and watsonx_ok:
        print("\n🎉 ALL SERVICES READY!")
        print("Now try: python -m app.graph.load_sample_data")
    else:
        print("\n⚠️ Fix the failed connections above, then retry")
    
    return neo4j_ok and astra_ok and watsonx_ok

if __name__ == "__main__":
    main()