# Lab 6.1 — Vector Store + RAG Retrieval (CP-15)

> 👋 **Module 6 first-timer?** Read [`docs/module-6-primer.md`](../docs/module-6-primer.md) first. About 20 minutes — explains RAG, vector embeddings, the system-prompt constraints.

> ℹ️ **Module:** 6 — GenAI STR Narrative Engine
> **Closes deficiency:** ARG-4 (foundation — retrieval must work before generation)
> **Time:** ~75 minutes if Milvus is up and the BGE model loads from cache; up to 3 hours if Milvus needs setup or model download is slow.
> **Source files:** [`src/ml/build_vector_store.py`](../src/ml/build_vector_store.py), [`src/ml/genai_rag_engine.py`](../src/ml/genai_rag_engine.py)

## What you're going to do

1. **Prepare the corpus directory** — `regulations/`, `exemplars/`, `rules/`. (~5 min)
2. **Verify Milvus is reachable** on the Cloudera AI cluster. (~3 min)
3. **Run the vector store builder** — embeds and inserts ~2,200 chunks. (~25 min — most is embedding compute)
4. **Run a representative retrieval query** — Case 0's layering pattern. (~10 min)
5. **Verify 4 representative pattern queries** retrieve relevant material. (~15 min)
6. **Verify CP-15 pass conditions** — four named checks. (~7 min)

Total: ~75 minutes.

## Before you begin — prerequisite checklist

- [ ] You have a CML workbench session with `pymilvus`, `sentence-transformers` available — quick check: `python -c "import pymilvus, sentence_transformers"` runs without error
- [ ] Milvus is deployed on the Cloudera AI cluster — your instructor handles this; quick check: `nc -zv ${MILVUS_HOST} 19530` succeeds
- [ ] The `${MILVUS_HOST}` env var is set in your workbench
- [ ] You have S3 read access (or local copy) of `s3://argus-training-assets/argus-capstone/v1.0/genai_corpus/`

## Why retrieval quality matters — read this before Step 3

**The RAG quality ceiling is set by the retrieval step.** If a query for "layering in mid-cap pharma" returns generic boilerplate instead of the specific PFUTP Reg 4(2)(e) text and a similar historical layering exemplar, **the LLM has nothing to ground on and will fabricate.**

This is the most important diagnostic in Module 6. Lab 6.1 verifies that retrieval surfaces the right context for the right pattern. Without that, Lab 6.2's generation is meaningless — the LLM will produce plausible-looking nonsense regardless of how well the system prompt is written. **Get retrieval right first.**

## Step 1 — Prepare the corpus directory

The corpus has three subdirectories. Your training environment should already have these populated; if not, fetch them:

```bash
ls -la data/genai_corpus/
```

**Expected:** three directories — `regulations/`, `exemplars/`, `rules/`. If missing or empty:

```bash
aws s3 sync s3://argus-training-assets/argus-capstone/v1.0/genai_corpus/ data/genai_corpus/
```

Then re-list:
```bash
ls data/genai_corpus/regulations/ | wc -l   # ~14 .txt files
ls data/genai_corpus/exemplars/ | wc -l     # 200 .json files
ls data/genai_corpus/rules/ | wc -l         # ~6 .md files
```

If any directory is empty after sync, your S3 access is denied — ask your instructor.

## Step 2 — Verify Milvus is reachable

```bash
nc -zv ${MILVUS_HOST} 19530
```

**Expected:** "succeeded" / "open".

If Milvus isn't reachable, your instructor sets it up via the CAI workbench's Milvus add-on. **This is not something the lab walks through** — Milvus deployment is a one-time CAI admin task. Confirm with your instructor before proceeding.

## Step 3 — Run the vector store builder

```bash
python src/ml/build_vector_store.py \
    --milvus-host ${MILVUS_HOST} \
    --corpus-dir data/genai_corpus/ \
    --rebuild
```

> 💡 **What `--rebuild` does:** drops any existing collection named `argus_${STUDENT_ID}_str_corpus` and creates a fresh one. Without `--rebuild`, the script appends — useful for incremental updates, but not for the initial build (you might end up with duplicates).

**Expected output** (10–25 minutes depending on whether the BGE model needs to download):

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

> 💡 **What you've just built:** a Milvus collection with 2,201 vectors. Each vector is a 768-dimensional float embedding of a chunk (a sentence/paragraph from regulations, an exemplar STR, or a rule definition). The collection has an HNSW index for fast cosine-similarity search. Querying takes milliseconds even for thousands of chunks.

If the build fails partway through, see Common Failure Mode #1.

## Step 4 — Run a representative retrieval query

In a Python notebook or workbench:

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

**Expected output:**

The top regulation hit should reference **PFUTP Reg 4(2)(e)** — the regulation that explicitly prohibits placing orders "without intention to be executed" — with a relevance score ≥ 0.65 (cosine similarity).

The top exemplar should be a layering-pattern STR with relevance score ≥ 0.60.

> 💡 **Reading cosine scores:** scores are 0.0–1.0 with the BGE model. Scores ≥ 0.65 are "highly relevant"; 0.50–0.65 are "moderately relevant"; below 0.50 are "weakly related." For RAG, you want the top-1 score to be ≥ 0.65 — anything lower means the LLM is getting noise as ground truth.

If the top hit is unrelated (e.g., a wash-trading regulation, or an exemplar for momentum ignition), see Common Failure Mode #2.

## Step 5 — Verify 4 representative pattern queries

Run the same retrieval pattern (modify `query`) for each of these and confirm results make sense:

| Query | Expected top regulation | Expected top exemplar pattern |
|---|---|---|
| `"spoofing single large order held then cancelled"` | PFUTP Reg 4(2)(e) | spoofing |
| `"marking the close index option pre-positioned"` | PFUTP Reg 4(2)(g) | marking-the-close |
| `"wash trading same member firm cross at same price"` | PFUTP Reg 4(2)(a) | wash trade |
| `"cross-product manipulation cash futures options imbalance"` | PFUTP Reg 4(2)(e) or (g) | cross-product |

Each query should return at least one regulation hit with cosine score ≥ 0.55 and at least one exemplar with cosine score ≥ 0.60.

> 💡 **Why these specific queries?** They mirror the patterns Module 3's R-101 through R-104 detect. Lab 6.2 will use the actual alert payload (which has `pattern_type` field) to drive retrieval — but you can preview here whether the corpus has good coverage for each pattern.

If any query returns weakly-related top hits (score < 0.50 across the board), the corpus is missing material for that pattern type — Common Failure Mode #2 covers the fix.

## Step 6 — Verify CP-15 pass conditions

CP-15 has **four checks**.

### Check 1 — Collection contains ≥ 2,000 chunks

```python
print(f"Collection size: {coll.num_entities}")
```
**Pass if:** ≥ 2,000. **Fail if:** smaller — re-run `--rebuild`.

### Check 2 — All three source types present

```python
for src in ("REGULATION", "EXEMPLAR", "RULE"):
    res = coll.query(expr=f'source_type == "{src}"', output_fields=["chunk_id"], limit=10)
    print(f"  {src}: {len(res) > 0}")
```
**Pass if:** True for all three. **Fail if:** any False — that source type's loader didn't run; check `build_vector_store.py` for that loader and re-run.

### Check 3 — Layering query returns PFUTP 4(2)(e) in top 3

The Step 4 retrieval returns at least one regulation hit referencing `4(2)(e)` in the top 3, with cosine score ≥ 0.65. **Pass if:** yes. **Fail if:** no — Common Failure Mode #2.

### Check 4 — All 4 representative queries find their expected exemplar

The Step 5 table's expected exemplar pattern shows up in the top 3 for every query. **Pass if:** all 4. **Fail if:** any miss — corpus quality issue.

---

## Common failure mode #1 — Build fails partway through (OOM or model download)

**Symptom:** `build_vector_store.py` exits with `OOM Error` or `URLError: connection timed out` to huggingface.co.

**Cause** (in decreasing likelihood):
1. **Workbench memory too low** — embedding 2,200 chunks in a batch needs ~2GB additional memory.
2. **HF download blocked** — air-gapped clusters can't download BGE.
3. **Milvus connection dropped mid-insert** — network blip causes partial collection.

**Fix sequence:**
1. **OOM:** request a 16GB workbench, restart kernel, retry.
2. **HF download:** ask your instructor for the offline BGE model checkpoint; place at `~/.cache/huggingface/hub/`. Then `transformers` will load from cache.
3. **Milvus drop:** drop the partial collection, retry:
   ```python
   from pymilvus import utility
   utility.drop_collection("argus_${STUDENT_ID}_str_corpus")
   ```
   Then re-run `--rebuild`.

## Common failure mode #2 — Exemplars retrieve well but regulations don't

**Symptom:** queries for "layering" return relevant exemplars (good), but the top regulation is a generic SEBI disclosure regulation rather than the manipulation-specific PFUTP Reg 4(2)(e).

**Cause:** the embedding model encodes prose well but struggles with regulatory jargon — the actual text of PFUTP Reg 4(2)(e) is dense legal language without the words "layering" or "spoofing", so cosine similarity to a query phrased in those terms is low. The regulatory corpus needs **augmented chunks**: short summaries of each regulation paired with the legal text.

**Diagnosis:**
```python
# Look at the actual top-1 regulation hit's text
hits = coll.search(data=qvec, anns_field="embedding",
                   param={"metric_type": "COSINE"}, limit=1,
                   expr='source_type == "REGULATION"',
                   output_fields=["section_ref", "text"])
print(hits[0][0].entity.get("text"))
```
If the text is dense legal prose with no matching keywords, this is the issue.

**Fix:** in `data/genai_corpus/regulations/`, create augmented files like `pfutp_reg_4_2_e_summary.txt`:

```
PFUTP Reg 4(2)(e) summary: prohibits placing buy or sell orders without intention
that the order be executed. Used to charge spoofing, layering, and momentum-ignition
manipulation patterns.

Original text:
"buying or selling of securities so as to operate only as a device to inflate, depress
or cause fluctuations in the price of such security..."
```

Then re-run `build_vector_store.py --rebuild`. The augmented chunks improve retrieval substantially because they contain both the plain-English keywords analysts query with *and* the citable legal text.

## Common failure mode #3 — `coll.load()` hangs

**Symptom:** the `coll.load()` call blocks for 30+ seconds without progressing.

**Cause:** Milvus is loading the collection's HNSW index from disk to memory. For 2,200 chunks this should take 2–5 seconds. If 30+, your Milvus pod is under-resourced.

**Fix:** ask your instructor to confirm the Milvus pod has at least 4GB memory. After Milvus restart, retry.

## Common failure mode #4 — Top exemplar score is 1.0 across all queries

**Symptom:** Step 4 / 5 show top exemplar score = 1.0 (or very close) regardless of query.

**Cause:** the exemplar embedding is unnormalized, or there's only one exemplar in the collection (so it always returns even if irrelevant).

**Diagnosis:**
```python
res = coll.query(expr='source_type == "EXEMPLAR"', limit=1000, output_fields=["chunk_id"])
print(f"Exemplar count: {len(res)}")
```
If <50, the exemplar loader didn't run completely.

**Fix:** check `build_vector_store.py`'s exemplar-loader logic; ensure the loop iterates all 200 JSON files. Re-run `--rebuild`.

---

## Pass condition for CP-15

All four checks pass:
- ✅ Collection contains ≥ 2,000 chunks
- ✅ All three source types present
- ✅ Layering query returns PFUTP 4(2)(e) in top 3
- ✅ All 4 representative queries find their expected exemplar

When all four pass, the LLM in Lab 6.2 will ground its drafts on **real, citable regulatory text + similar historical exemplars** — not its training-time priors about Indian securities law.

## Wrap-up — what you can now do that you couldn't before

You can build a Milvus vector store from a multi-source regulatory corpus. You can run cosine-similarity retrieval against the store and interpret relevance scores. You can diagnose retrieval quality issues (corpus gaps, embedding-model mismatches, augmentation needs). You understand why **retrieval quality is the ceiling on RAG output quality**.

Lab 6.2 puts the LLM on top — generating actual STR drafts from alert payloads, with the 5 system-prompt constraints. Allow ~90 minutes.
