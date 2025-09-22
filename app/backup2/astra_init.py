import os
from dotenv import load_dotenv
from astrapy import DataAPIClient

load_dotenv()

client = DataAPIClient()
db = client.get_database(
    os.getenv("ASTRA_DB_API_ENDPOINT"), 
    token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"),
    keyspace="well_planning"
)

name = os.getenv("ASTRA_DB_VECTOR_COLLECTION", "drilling_docs")

# Create collection with vectorize enabled
if name not in db.list_collection_names():
    try:
        db.create_collection(
            name,
            dimension=1536,
            metric="cosine",
            service={
                "provider": "openai",
                "modelName": "text-embedding-3-small"
            }
        )
        print(f"✅ Created collection with vectorize: {name}")
    except Exception as e:
        # Fallback to basic collection
        db.create_collection(name, dimension=1536, metric="cosine")
        print(f"⚠️ Created basic collection: {name}")
        print("💡 Configure Vectorize manually in AstraDB console")
else:
    print(f"✅ Collection already exists: {name}")

# Test the collection
try:
    coll = db.get_collection(name)
    print(f"✅ Collection accessible: {name}")
except Exception as e:
    print(f"❌ Collection access failed: {e}")