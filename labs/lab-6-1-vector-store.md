# Lab 6.1 — Vector Store + RAG Retrieval (CP-15)

> ℹ️ **Module:** 6 — GenAI STR Narrative Engine
> **Closes deficiency:** ARG-4 (foundation — retrieval must work before generation)
> **Source files:** [`src/ml/build_vector_store.py`](../src/ml/build_vector_store.py), [`src/ml/genai_rag_engine.py`](../src/ml/genai_rag_engine.py)

## Objectives

- Place the regulatory corpus + 200 exemplar STRs + ESM/ASM rule definitions on disk in `data/genai_corpus/`
- Run `build_vector_store.py` to ingest the corpus into Milvus on Cloudera AI
- Verify retrieval queries return relevant chunks for representative manipulation patterns

## Why this matters

The RAG quality ceiling is set by the retrieval step. If a query for "layering in mid-cap pharma" returns generic boilerplate instead of the specific PFUTP Reg 4(2)(e) text and a similar historical layering exemplar, the LLM has nothing to ground on and will fabricate. Lab 6.1 verifies that the retrieval layer surfaces the right context for the right pattern — without which Lab 6.2's generation is meaningless.

## Procedure

### Step 1 — Prepare the corpus directory

The corpus has three subdirectories. Your training environment should already have these populated; if not, your instructor's S3 asset bundle (`s3://argus-training-assets/argus-capstone/v1.0/genai_corpus/`) contains all three.

```bash
ls -la data/genai_corpus/
# expect:
#   regulations/   — ~14 .txt files (SEBI Master Circular chapters + PFUTP regs)
#   exemplars/     — 200 .json files (approved historical STRs)
#   rules/         — ESM/ASM rule definitions in .md
```

If any of those directories are missing or empty, fetch the asset bundle:

```bash
aws s3 sync s3://argus-training-assets/argus-capstone/v1.0/genai_corpus/ data/genai_corpus/
```

### Step 2 — Verify Milvus is reachable

The lab assumes Milvus is already deployed on the Cloudera AI cluster. Confirm:

```bash
nc -zv ${MILVUS_HOST} 19530
# expect: succeeded / open
```

If Milvus isn't reachable, your instructor sets it up via the CAI workbench's Milvus add-on. This is *not* something the lab walks through — Milvus deployment is a one-time CAI admin task.

### Step 3 — Run the vector store builder

```bash
python src/ml/build_vector_store.py \
    --milvus-host ${MILVUS_HOST} \
    --corpus-dir data/genai_corpus/ \
    --rebuild
```

**What you should see**: progress logs reporting chunk counts per source type and the embedding-model load. The full run takes 10–25 minutes depending on whether the BGE model needs to download.

```
==> Connecting to Milvus at milvus.argus.local:19530
==> Loading embedding model: BAAI/bge-base-en-v1.5
==> Collection argus_${STUDENT_ID}_str_corpus created (dim=768, HNSW/COSINE)
==> Loading regulation corpus from data/genai_corpus/regulations
    1,847 regulation chunks
==> Loading exemplar STRs from data/genai_corpus/exemplars
      200 exemplar STRs
==> Loading rules from data/genai_corpus/rules
      154 rule chunks
==> Embedding + inserting 2,201 chunks into argus_${STUDENT_ID}_str_corpus
==> Inserted 2,201 chunks. Collection ready for queries.
```

### Step 4 — Run a representative retrieval query

In a Python session (CML notebook or local):

```python
from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer

connections.connect("default", host="${MILVUS_HOST}", port="19530")
coll = Collection("argus_${STUDENT_ID}_str_corpus"); coll.load()
embedder = SentenceTransformer("BAAI/bge-base-en-v1.5")

# A representative query — Case 0's pattern
query = "layering pattern mid-cap pharma stacked non-bona-fide orders cancelled within 200ms"
qvec = embedder.encode([query], normalize_embeddings=True).tolist()

# Retrieve top-5 regulations
hits_reg = coll.search(
    data=qvec, anns_field="embedding",
    param={"metric_type": "COSINE", "params": {"ef": 64}},
    limit=5,
    expr='source_type == "REGULATION"',
    output_fields=["section_ref", "text"],
)
print("=== TOP REGULATIONS ===")
for h in hits_reg[0]:
    print(f"  [{h.score:.4f}] {h.entity.get('section_ref')}: {h.entity.get('text')[:120]}")

# Retrieve top-3 exemplars
hits_ex = coll.search(
    data=qvec, anns_field="embedding",
    param={"metric_type": "COSINE", "params": {"ef": 64}},
    limit=3,
    expr='source_type == "EXEMPLAR"',
    output_fields=["section_ref", "text"],
)
print("\n=== TOP EXEMPLARS ===")
for h in hits_ex[0]:
    print(f"  [{h.score:.4f}] {h.entity.get('section_ref')}: {h.entity.get('text')[:120]}")
```

**Expected output**:

The top regulation hit should reference **PFUTP Reg 4(2)(e)** — the regulation that explicitly prohibits placing orders "without intention to be executed" — with a relevance score ≥ 0.65 (cosine similarity).

The top exemplar should be a layering-pattern STR with relevance score ≥ 0.60.

If the top hit is unrelated (e.g. a wash trading regulation, or an exemplar for momentum ignition), the embedding model isn't separating manipulation patterns well — the most common cause is the embedding model loaded the wrong checkpoint or the corpus has incorrect `section_ref` metadata. Re-check the regulation file naming conventions.

### Step 5 — Verify 4 representative pattern queries

Run retrievals for the other key pattern types and confirm the results make sense:

| Query | Expected top regulation | Expected top exemplar pattern |
|---|---|---|
| "spoofing single large order held then cancelled" | PFUTP Reg 4(2)(e) | spoofing |
| "marking the close index option pre-positioned" | PFUTP Reg 4(2)(g) | marking-the-close |
| "wash trading same member firm cross at same price" | PFUTP Reg 4(2)(a) | wash trade |
| "cross-product manipulation cash futures options imbalance" | PFUTP Reg 4(2)(e) or (g) | cross-product |

Each query should return at least one regulation hit with cosine score ≥ 0.55.

## Checkpoint CP-15 — RAG retrieval works

### Pass condition

All four checks pass.

### Check 1 — Collection contains ≥ 2,000 chunks

```python
print(f"Collection size: {coll.num_entities}")
# expect: >= 2000
```

### Check 2 — All three source types present

```python
for src in ("REGULATION", "EXEMPLAR", "RULE"):
    res = coll.query(expr=f'source_type == "{src}"', output_fields=["chunk_id"], limit=10)
    print(f"  {src}: {len(res) > 0}")
# expect: True for all three
```

### Check 3 — Layering query returns PFUTP 4(2)(e) in top 3

The Step 4 retrieval returns at least one regulation hit referencing `4(2)(e)` in the top 3, with cosine score ≥ 0.65.

### Check 4 — At least one exemplar with cosine score ≥ 0.6 for each of the 4 representative queries

The Step 5 table's expected exemplar shows up in the top 3 for every query.

---

## Common failure mode — exemplars retrieve well but regulations don't

**Symptom**: queries for "layering" return relevant exemplars (good), but the top regulation is a generic SEBI disclosure regulation rather than the manipulation-specific PFUTP Reg 4(2)(e).

**Diagnosis**: the embedding model encodes prose well but struggles with regulatory jargon — the actual text of PFUTP Reg 4(2)(e) is dense legal language without the words "layering" or "spoofing", so cosine similarity to a query phrased in those terms is low. The regulatory corpus needs *augmented* chunks: short summaries of each regulation paired with the legal text.

**Fix**: in `data/genai_corpus/regulations/`, create augmented files like `pfutp_reg_4_2_e_summary.txt` that combine plain-English summaries with the legal text:

```
PFUTP Reg 4(2)(e) summary: prohibits placing buy or sell orders without intention
that the order be executed. Used to charge spoofing, layering, and momentum-ignition
manipulation patterns.

Original text:
"buying or selling of securities so as to operate only as a device to inflate, depress
or cause fluctuations in the price of such security..."
```

Then re-run `build_vector_store.py --rebuild`. The augmented chunks improve retrieval substantially because they contain both the plain-English keywords analysts query with *and* the citable legal text.

---

## Pass condition for CP-15

All four checks pass. With retrieval working, the LLM in Lab 6.2 will ground its drafts on real, citable regulatory text + similar historical exemplars — not its training-time priors about Indian securities law.
