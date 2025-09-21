import os, argparse, requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup as BS
from dotenv import load_dotenv
from astrapy import DataAPIClient
from app.vector.doc_parsers import read_text_any

load_dotenv()

def _collection():
    client = DataAPIClient()
    db = client.get_database(os.getenv("ASTRA_DB_API_ENDPOINT"), token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
    return db.get_collection(os.getenv("ASTRA_DB_VECTOR_COLLECTION","drilling_docs"))

def _read_local(path: str):
    return read_text_any(path)

def _fetch_url(url: str):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    ct = r.headers.get("content-type","").lower()
    if "text/html" in ct:
        return BS(r.text, "html.parser").get_text(" ")
    if any(x in ct for x in ["text/plain", "markdown"]):
        return r.text
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="*", default=[])
    ap.add_argument("--urls", nargs="*", default=[])
    ap.add_argument("--source_tag", default="public")
    args = ap.parse_args()

    coll = _collection()
    docs = []

    for fp in args.files:
        body = _read_local(fp)
        doc = {"source": args.source_tag, "path": fp}
        if body: doc["body"] = body
        docs.append(doc)

    for url in args.urls:
        body = _fetch_url(url)
        doc = {"source": args.source_tag, "url": url}
        if body: doc["body"] = body
        docs.append(doc)

    if not docs:
        raise SystemExit("No docs to ingest.")

    for d in docs:
        coll.insert_one(d)
    print(f"Inserted {len(docs)} docs into AstraDB.")

if __name__ == "__main__":
    main()
