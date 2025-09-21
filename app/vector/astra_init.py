import os, glob
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
dim = int(os.getenv("ASTRA_DB_VECTOR_DIM", "1024"))

# Create basic vector collection
if name not in db.list_collection_names():
    db.create_collection(
        name, 
        definition={
            "vector": {"dimension": dim, "metric": "cosine"}
        }
    )
    print(f"Created basic collection: {name}")
    print("💡 Configure Vectorize manually in AstraDB console")
else:
    print(f"Using existing collection: {name}")

# Insert documents
coll = db.get_collection(name)
for path in glob.glob("data/docs/*.md"):
    body = open(path, "r").read()
    coll.insert_one({"path": path, "body": body, "source": "seed"})
print("Inserted seed docs.")
