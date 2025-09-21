# Well Planning Generation — LangGraph + watsonx.ai + AstraDB + Neo4j
Well Planning Generation — LangGraph + IBM watsonx.ai + AstraDB + Neo4j
============================================================================

This README is the single source of truth for installing, configuring, and running
the Well Planning Generation tutorial and related project files. It includes a
concise summary, step-by-step guide (human-readable, no code dumps), commands,
and references to every script in the repository. Use it as your runbook.

-------------------------------------------------------------------------------
0) TL;DR SUMMARY
-------------------------------------------------------------------------------
You will run a LangGraph agent that drafts, validates, and iterates well plans.
It pulls authoritative, structured context from a Neo4j knowledge graph (wells,
formations, BHAs, constraints, historical plans) and unstructured guidance from
AstraDB (manuals, best-practices, and research papers). IBM watsonx.ai (Granite)
generates and refines plans. Evidence weights bias the model toward graph facts
vs. document snippets. A strict policy prohibits degraded behavior; Astra vector
search MUST use server-side vectorize, or the run will stop with a clear error.

Key components:
- LangGraph loop: retrieve → draft → validate → reflect (repeat until pass or cap)
- GraphRAG (Neo4j): deterministic joins, constraints, audit trail
- RAG (AstraDB): unstructured narrative snippets via server-side vectorize
- IBM watsonx.ai Granite: generation and targeted reflection
- Connectors: NSTA (UK), BOEM (US offshore sample), RRC (TX CSV), USGS grid sample,
  NPD (Norway sample) to expand the KG
- Papers pipeline: upload open-access PDFs → Neo4j (:Paper, :Topic) + Astra text
- CI/audit: repo enforces no degraded behavior and bans unsafe patterns

-------------------------------------------------------------------------------
1) PREREQUISITES
-------------------------------------------------------------------------------
- Python 3.11+
- An AstraDB instance with Data API enabled and a collection with *server-side
  vectorize* ON (required)
- A Neo4j 5+ instance (Aura or self-hosted) with credentials
- IBM watsonx.ai account & project with access to Granite instruct model
- Internet access for connectors & document ingestion (optional, for enrichment)

-------------------------------------------------------------------------------
2) DIRECTORY LAYOUT (TOP-LEVEL)
-------------------------------------------------------------------------------
app/
  agent/workflow.py            ← LangGraph graph (retrieve/draft/validate/reflect)
  llm/watsonx_client.py        ← Granite client wrapper
  graph/graph_rag.py           ← GraphRAG + Astra snippet retrieval (strict)
  graph/neo4j_schema.cypher    ← Constraints & indexes
  graph/load_sample_data.py    ← Seed KG with sample CSVs
  vector/astra_init.py         ← Create/use collection, seed docs
  vector/astra_doc_ingest.py   ← Ingest local files & URLs into Astra
  vector/doc_parsers.py        ← Open-source parsers (pdfminer/docx/pptx/xlsx/html)
  vector/astra_hybrid_search.py← CLI to test server-side vector search (strict)
  evaluation/kpi.py            ← Simple KPI proxy (replace with your model)
  connectors/*.py              ← NSTA/BOEM/RRC/USGS/NPD loaders into Neo4j
  examples/scrape_ingest_and_search.py ← Minimal scrape→ingest→retrieve demo
  papers/paper_uploader.py     ← PDF→Neo4j(:Paper,:Topic)+Astra (strict, topics auto)
  papers/topic_suggester.py    ← Local TF‑IDF topics (no external services)
clients/requests.http          ← VS Code REST client file
data/*.csv, data/docs/*.md     ← Sample seeds for KG and Astra
docs/ARCHITECTURE.md, SOURCES.md← Extra background
tools/audit_no_degrade.py      ← Policy audit (no degraded behavior, etc.)
tools/ci_smoke.py              ← Imports core modules (no network)
.github/workflows/ci.yml       ← CI pipeline (audit, syntax check, smoke import)
Makefile                       ← Common tasks
README.md (repo)               ← Short overview (this README.txt is the full runbook)

-------------------------------------------------------------------------------
3) ENVIRONMENT VARIABLES (CONFIGURE .env)
-------------------------------------------------------------------------------
IBM watsonx.ai
  WX_API_KEY           Your API key
  WX_URL               ex: https://us-south.ml.cloud.ibm.com
  WX_PROJECT_ID        Your project GUID
  WX_MODEL_ID          ex: ibm/granite-13b-instruct-v2

AstraDB (Data API)
  ASTRA_DB_API_ENDPOINT        ex: https://<db-id>-<region>.apps.astra.datastax.com
  ASTRA_DB_APPLICATION_TOKEN   ex: AstraCS:...
  ASTRA_DB_VECTOR_COLLECTION   ex: drilling_docs
  ASTRA_DB_VECTOR_DIM          ex: 1024
  ASTRA_USE_SERVER_VECTORIZE   MUST be true (true/1/yes)

Neo4j
  NEO4J_URI             ex: neo4j+s://<your-instance>.databases.neo4j.io
  NEO4J_USER            ex: neo4j
  NEO4J_PASSWORD        ex: ******

Prompt Evidence Weights
  GRAPH_WEIGHT          default 0.7  (higher = more graph emphasis)
  ASTRA_WEIGHT          default 0.3  (higher = more doc emphasis)

-------------------------------------------------------------------------------
4) INSTALLATION
-------------------------------------------------------------------------------
1. Create a virtual environment and install dependencies:
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt

2. Create and populate .env:
   cp .env.example .env
   # edit with your real values (Astra, Neo4j, watsonx)

3. Prepare Astra collection (server-side vectorize required):
   - Ensure your collection has vectorize enabled in Astra UI.
   - Set ASTRA_USE_SERVER_VECTORIZE=true in your .env.

-------------------------------------------------------------------------------
5) SEED BOTH STORES
-------------------------------------------------------------------------------
AstraDB (vector RAG):
  python -m app.vector.astra_init
  # Seeds two small docs in data/docs/

Neo4j (GraphRAG):
  python -m app.graph.load_sample_data
  # Loads schema/indexes and CSV seeds in data/

-------------------------------------------------------------------------------
6) RUN THE SERVICE
-------------------------------------------------------------------------------
Start the API:
  uvicorn app.main:app --reload --port 8008

Try the agent (VS Code REST client):
  Open clients/requests.http and click “Send Request”, or use curl:
  curl -X POST http://localhost:8008/plan/run -H "Content-Type: application/json" \
       -d '{"well_id":"W-1001","objectives":"Minimize cost & vibration","max_loops":3}'

What happens:
  1) retrieve: fetch Neo4j subgraph + top-3 Astra snippets (strict vectorize)
  2) draft: watsonx.ai Granite produces a plan (prompt highlights weights)
  3) validate: constraint gate + KPIs (replace logic with your model)
  4) reflect: one targeted change if not compliant; loop until pass or cap

-------------------------------------------------------------------------------
7) EVIDENCE WEIGHTS (HOW THEY INFLUENCE OUTPUT)
-------------------------------------------------------------------------------
- GRAPH_WEIGHT vs ASTRA_WEIGHT control the *prompt structure & emphasis*.
- The higher-weighted block appears first and gets more textual “budget”.
- The prompt includes a note like “Evidence weights → Graph:0.70 Astra:0.30”
  to steer conflict resolution toward the higher-weighted source.
- Validation still enforces constraints regardless of weights.

Set temporarily per shell:
  export GRAPH_WEIGHT=0.60
  export ASTRA_WEIGHT=0.40

Or via Makefile:
  make evidence-weights GRAPH=0.6 ASTRA=0.4  &&  make run

-------------------------------------------------------------------------------
8) ADD PUBLIC DATA (OPTIONAL, FOR A RICHER KG)
-------------------------------------------------------------------------------
All connectors write into Neo4j. Samples are provided for easy testing:

UK NSTA offshore wells (ArcGIS service; JSON API):
  python -m app.connectors.nsta_offshore_wells --limit 50

BOEM Gulf of Mexico sample (CSV included):
  python -m app.connectors.boem_wells --sample data/external_samples/boem_offshore_wells_sample.csv

Texas RRC (your CSV extract):
  python -m app.connectors.rrc_loader --csv path/to/rrc.csv --limit 200

USGS aggregated drilling history sample (CSV included):
  python -m app.connectors.usgs_aggregated

Norway NPD sample (CSV included):
  python -m app.connectors.npd_wellbores --sample data/external_samples/npd_wellbores_sample.csv

Notes:
- These enrich the KG with Well/Region nodes and attributes for better GraphRAG.
- You can extend loaders to map additional attributes (field, operator, coords).

-------------------------------------------------------------------------------
9) INGEST DOCUMENTS FOR RAG (ASTRA)
-------------------------------------------------------------------------------
Use open-source parsers only (as per requirement). Supported local file types:
- PDF (pdfminer.six), DOCX (python-docx), PPTX (python-pptx), XLSX (openpyxl),
  HTML (BeautifulSoup), MD/TXT

Local files:
  python -m app.vector.astra_doc_ingest --files path/to/file1.pdf path/to/notes.md

Public URLs (HTML/text):
  python -m app.vector.astra_doc_ingest --urls https://opendata-nstauthority.hub.arcgis.com/pages/well-data

Mini demo (scrape→ingest→retrieve context):
  python -m app.examples.scrape_ingest_and_search --url https://opendata-nstauthority.hub.arcgis.com/pages/well-data --well_id W-1001

STRICT REQUIREMENT: Server-side vectorize must be enabled, or tools will abort.

-------------------------------------------------------------------------------
10) RESEARCH PAPERS → KG + ASTRA
-------------------------------------------------------------------------------
Use the paper uploader to add open-access PDFs into both the KG and vector DB.
- Extracts title/year/DOI from the first page (pdfminer.six).
- Upserts (:Paper) and (:Topic) nodes in Neo4j (topics auto if omitted).
- Ingests the PDF text into Astra for retrieval.

Examples:
  # auto topics (local TF‑IDF)
  python -m app.papers.paper_uploader --pdf path/to/paper.pdf --source NETL

  # multiple PDFs w/ explicit topics
  python -m app.papers.paper_uploader --pdfs "data/papers/*.pdf" --topics "ROP, stick-slip" --source arXiv

The topic suggester is local (scikit-learn TF‑IDF) and uses no network.

-------------------------------------------------------------------------------
11) CI / POLICY AUDIT / MAKEFILE
-------------------------------------------------------------------------------
Make targets:
  make install           # install dependencies
  make seed              # Astra + Neo4j sample seeds
  make run               # start API
  make connectors-*      # run any connector (see Makefile)
  make evidence-weights  # echo the current env weight settings
  make audit             # run policy audit

CI (GitHub Actions):
  - Installs dependencies
  - Runs tools/audit_no_degrade.py
  - Syntax compiles all Python modules
  - Imports core modules (smoke test; no network calls)

Policy audit (no degraded behavior):
  - Requires Astra server-side vectorize; tools exit on misconfiguration
  - Bans the word “fallback” in code/docs/yaml
  - Bans silent `except: pass` patterns
  - Flags suspicious `if ... or True:` constructs

-------------------------------------------------------------------------------
12) TROUBLESHOOTING
-------------------------------------------------------------------------------
Astra vector query fails:
  • Ensure ASTRA_USE_SERVER_VECTORIZE=true in .env
  • Verify your collection has vectorize enabled in Astra console
  • Confirm ASTRA_DB_API_ENDPOINT and ASTRA_DB_APPLICATION_TOKEN

Neo4j connection errors:
  • Confirm NEO4J_URI/USER/PASSWORD and allow-list your IP for Aura
  • Ensure you ran app/graph/load_sample_data.py to create constraints

watsonx.ai response empty:
  • Check WX_API_KEY/WX_URL/WX_PROJECT_ID and model ID
  • Ensure the Granite model is available to your project

Agent never converges:
  • Increase MAX_LOOPS (request body)
  • Raise GRAPH_WEIGHT for more conservative plans
  • Add higher-quality constraints/historical plans to the KG

-------------------------------------------------------------------------------
13) WHAT GOES WHERE (RAG VS. KG)
-------------------------------------------------------------------------------
Neo4j (GraphRAG, authoritative & relational):
  Well, Formation, BHATool, EngineeringConstraint, HistoricalPlan, PlanIteration,
  KPI, Paper, Topic (+ edges). Deterministic joins; complete subgraph retrieval.

AstraDB (vector RAG, narrative & experiential):
  Seed docs, manuals, best practices, incidents, manufacturer envelopes,
  open-access research PDFs. Retrieval is top‑k snippets via server-side vectorize.

Validation is the hard gate regardless of weights: a plan must meet constraints.

-------------------------------------------------------------------------------
14) API CONTRACT
-------------------------------------------------------------------------------
POST /plan/run
Request JSON:
  {
    "well_id": "W-1001",
    "objectives": "Minimize cost and vibration while achieving target ROP",
    "max_loops": 3
  }
Response JSON (shape):
  {
    "plan_id": "plan-...",
    "kpis": {...},
    "validation": {"passes": true/false, ...},
    "plan": "<model-generated content>"
  }

-------------------------------------------------------------------------------
15) EXTENDING THE SYSTEM
-------------------------------------------------------------------------------
- Replace the simple validator/KPIs with your physics/ML models.
- Enrich the KG with regulatory data and manufacturer specs.
- Add more ingestion pipelines (still using open-source parsers or IBM tools).
- Add observability (persist prompts/weights and decision traces to Neo4j).

-------------------------------------------------------------------------------
16) LICENSE / CREDITS
-------------------------------------------------------------------------------
This is a teaching/reference project. Verify all external data sources are used
under their respective terms. Prefer open-access sources for automated ingestion.