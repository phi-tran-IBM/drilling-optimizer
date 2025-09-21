import os, argparse
from dotenv import load_dotenv
from astrapy import DataAPIClient

load_dotenv()

def _collection():
    client = DataAPIClient()
    db = client.get_database(os.getenv("ASTRA_DB_API_ENDPOINT"), token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
    return db.get_collection(os.getenv("ASTRA_DB_VECTOR_COLLECTION", "drilling_docs"))

def vector_mode_enabled():
    return os.getenv("ASTRA_USE_SERVER_VECTORIZE", "false").lower() in ["1","true","yes"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--k", type=int, default=5)
    args = ap.parse_args()

    if not vector_mode_enabled():
        raise SystemExit("Server-side vectorize is required. Set ASTRA_USE_SERVER_VECTORIZE=true and enable vectorize on the collection.")
    coll = _collection()
    res = coll.find({}, options={"limit": args.k, "includeSimilarity": True, "sort": {"$vectorize": args.query}})
    items = list(res)
    print(f"top {len(items)} results:")
    for d in items:
        sim = d.get("$similarity")
        path = d.get("path") or d.get("url")
        print(f"- similarity={sim:.4f} source={d.get('source')} doc={path}")

if __name__ == "__main__":
    main()
