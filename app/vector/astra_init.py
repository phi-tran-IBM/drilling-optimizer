import os
from dotenv import load_dotenv
from astrapy import DataAPIClient

load_dotenv()

client = DataAPIClient()
db = client.get_database(
    os.environ["ASTRA_DB_API_ENDPOINT"], 
    token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
    keyspace="well_planning"
)

name = os.environ["ASTRA_DB_VECTOR_COLLECTION"]

# Create collection with vectorize enabled - fail fast if unable
if name not in db.list_collection_names():
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
else:
    print(f"✅ Collection already exists: {name}")

# Test the collection - fail fast if inaccessible
coll = db.get_collection(name)
print(f"✅ Collection accessible: {name}")