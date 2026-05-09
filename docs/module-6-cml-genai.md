# Module 6 — GenAI STR Narrative Engine (RAG)

> 📊 **Visual reference**: [Module 6 RAG narrative pipeline](../assets/diagrams/07_module6_cml_genai.md) ([SVG](../assets/diagrams/07_module6_cml_genai.svg))

> **Closes deficiency:** ARG-4 — Suspicious Transaction Report narratives are entirely hand-written, taking 40+ minutes per case
> **Day:** 8
> **Checkpoints:** CP-15, CP-16
> **Weight:** 15% of capstone

## What's broken

When a surveillance analyst confirms a suspicious pattern, they must produce a written narrative — the Suspicious Transaction Report (STR) — describing the manipulation, the order-flow sequence, the price impact, the suspected intent, and the regulation alleged to have been violated. This narrative goes to MSE Compliance, then to SEBI. Each narrative currently takes 40–90 minutes to draft. The backlog of confirmed-but-undocumented cases stands at 340 reports, oldest 70 days old. SEBI's Master Circular requires reports filed "within reasonable time of detection" and has explicitly cited MSE's documentation backlog as a separate finding in the Show Cause Notice.

The cognitive task is mostly translation: turning a structured event sequence into prose that a SEBI investigator can follow. Boilerplate is identical across reports. It is exactly the work a language model can draft in seconds and a human can review and finalize in minutes.

## What you build

A retrieval-augmented generation (RAG) pipeline that produces a structured JSON STR draft for every confirmed alert. The pipeline:

1. Pulls the alert's structured payload from `argus_${STUDENT_ID}_gold.alert_candidates`
2. Retrieves the top-5 relevant SEBI / PFUTP regulatory citations from a Milvus vector store
3. Retrieves the top-3 most similar historical exemplar STRs that Compliance has previously approved
4. Fetches news headlines for the instrument over the ±24-hour alert window
5. Assembles a grounded prompt with five locked system-prompt constraints
6. Calls a CAII-hosted open-weights LLM (Llama 3.1 70B or Mistral Large)
7. Parses the response into a strict JSON schema (5 required sections)
8. Falls back to a deterministic template-fill if the LLM returns malformed JSON twice — never silently fails

The five locked system-prompt constraints, lifted from PRD §9 verbatim:

1. **No fabrication** — prices, volumes, member names come ONLY from the structured payload
2. **Cite the regulation** — specific PFUTP regulation reference (e.g., "Reg 4(2)(e)")
3. **Describe behavior, not intent** — only SEBI can find intent; the draft must restrict itself to observable behavior
4. **No comparison to named cases** — never invoke historical SEBI orders or named individuals
5. **Output in English** — SEBI's working language, regardless of input language in news context

These constraints are split into a **system prompt** (immutable, sent verbatim on every call) and a **per-alert grounding template** (filled at runtime). The split lets compliance review the system prompt once and trust it on every call, regardless of what the alert payload contains.

Critically: every generated narrative is **reviewed and edited by an analyst before submission**. The system never auto-files an STR. The success metric is analyst time saved per report (target: 60 min → 8 min, ~87% reduction), not autonomy.

## CDP services used

- **Cloudera AI Inference (CAII)** — hosts the open-weights LLM (Llama 3.1 70B or Mistral Large) on GPU pool; data sovereignty for DPDP §16
- **Milvus** — vector store running on Cloudera AI; HNSW index over BGE-base-en-v1.5 embeddings
- **Cloudera Data Engineering** — orchestrates the per-alert RAG calls
- **Cloudera Data Visualization** — renders the structured JSON into the analyst review UI

## Source files

| File | Purpose |
|---|---|
| [`src/ml/system_prompts.py`](../src/ml/system_prompts.py) | Locked 5-constraint system prompt + grounding template + fallback template |
| [`src/ml/build_vector_store.py`](../src/ml/build_vector_store.py) | One-time / monthly Milvus ingest of regulatory corpus + 200 exemplar STRs |
| [`src/ml/genai_rag_engine.py`](../src/ml/genai_rag_engine.py) | Per-alert RAG pipeline: retrieve → ground → call LLM → parse JSON → fallback |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 6.1 — Vector Store + RAG Retrieval](../labs/lab-6-1-vector-store.md) | Build Milvus collection; verify retrieval returns relevant SEBI citations | CP-15 |
| [Lab 6.2 — STR Generation](../labs/lab-6-2-str-generation.md) | Generate 5 STR drafts; verify schema, no fabrication, correct PFUTP citation | CP-16 |

## Measurable outcome

By end of module:

- Milvus collection `argus_${STUDENT_ID}_str_corpus` has at least 2,500 chunks across the three source types (REGULATION / EXEMPLAR / RULE)
- A retrieval query for "layering in mid-cap pharma" returns at least one chunk citing PFUTP Reg 4(2)(e) and at least one exemplar STR for a layering case
- Five confirmed alerts (Cases 0, 1, 2, 4, 5 — the real planted manipulations) each produce a valid JSON STR draft
- Every draft includes all 5 required sections; none fabricates a member firm name or price not in the alert payload
- Every draft cites a specific PFUTP regulation reference matching the pattern type
- The fallback template fires zero times under normal operation; it fires when the LLM endpoint is unreachable, producing a usable skeleton with `narrative_generation_failed=true`

## What this fixes

Before ARGUS, an analyst confirming a layering case at 10:30am would close their morning at 11:30am with the STR drafted and submitted to Compliance. After ARGUS, the same workflow is: open the alert at 10:30, click "Draft STR", a JSON arrives in 8 seconds, the analyst reads it, edits one paragraph for tone, clicks Submit. The 60-minute task becomes an 8-minute task. Across MSE's surveillance team, the 340-report documentation backlog drains in three weeks, and the team spends their reclaimed time on actual investigation.

> 💡 **Tip:** The most common reason an STR draft fails review is that the LLM hallucinated a value not in the alert payload. The system prompt explicitly forbids this; if you see fabrication during testing, check that the grounding template is filling all the structured-payload fields literally — a single missing `{member_firm_name}` slot lets the LLM "fill the gap" and that's where hallucination starts.

> ⚠️ **Compliance gate:** The LLM endpoint MUST be a locally-hosted CAII model. DPDP §16 + RBI cloud-services guidance prohibit sending personal data of Indian data principals to external AI services. Do not modify `genai_rag_engine.py` to call OpenAI, Anthropic, or any other external API — even for testing convenience. The data residency posture is what makes the entire platform DPDP-defensible; compromising it for one module compromises the whole.
