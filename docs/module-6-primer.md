# Module 6 Primer — Read This Before Lab 6.1

> 📊 **Visual reference**: [Module 6 GenAI STR pipeline](../assets/diagrams/07_module6_cml_genai.md) ([SVG](../assets/diagrams/07_module6_cml_genai.svg))

> 👋 **New to RAG, vector stores, LLM prompting, or hallucination defenses?** This primer is for you. About 20 minutes. Module 6 has the most external concepts in the capstone after Module 5.

This is a **primer**, not a procedure. The actual hands-on work is in Module 6's two labs. Read this first.

## The big picture in one paragraph

Module 6 fixes ARG-4 — the **40-minute hand-written STR (Suspicious Transaction Report) bottleneck** that separately drove SEBI's Show Cause findings. Two labs: build a vector store from a regulatory corpus + 200 historical exemplar STRs (Lab 6.1), then use Retrieval-Augmented Generation (RAG) to draft STRs grounded in retrieved regulatory text and similar exemplars (Lab 6.2). By the end of Module 6, the analyst's 60-minute hand-drafting task collapses to an **8-minute review** of an LLM-generated draft. **Crucially**, the LLM is constrained by 5 locked system-prompt rules — no fabricated numbers, no claimed intent, no named cases, no non-English output, no missing PFUTP citation — so the speedup doesn't come at the cost of regulatory exposure.

## Concepts you'll meet

### What an STR is, in one paragraph

A **Suspicious Transaction Report** is a regulatory filing the exchange must submit to SEBI within 7 days of identifying suspected manipulation. The STR has a fixed structure: executive summary, order-flow narrative, quantified market impact, suspected violation citation (a PFUTP regulation), recommended next steps. Each field has a content rule — the executive summary must be 60 words max, the citation must be a specific PFUTP sub-section, etc. Drafting an STR by hand from a 60-feature alert payload takes a senior analyst 40–60 minutes per draft. MSE's 28-person team accumulates a backlog measured in *weeks*; the SEBI Show Cause cited 14 manipulation cases where MSE missed the 7-day filing deadline.

### Retrieval-Augmented Generation (RAG)

LLMs trained on internet text don't reliably know Indian securities law. Asked to draft an STR citing PFUTP Regulation 4(2)(e), a base LLM might cite a regulation that doesn't exist, or paraphrase one that exists but doesn't match the pattern. **RAG fixes this** by retrieving relevant regulatory text and similar historical exemplars at query time and injecting them into the prompt. The LLM grounds its draft on the retrieved text instead of relying on training-time priors.

The pipeline:
1. Embed the alert's structured payload (rule, instrument, pattern type, severity, top SHAP features) as a query vector.
2. Retrieve the top-k similar regulatory chunks and exemplar STRs from the vector store.
3. Inject retrieved chunks + the alert payload into a system prompt with the 5 locked constraints.
4. Call the LLM. Get back a structured JSON STR draft.
5. Validate the JSON. If any constraint is violated, regenerate or fall back.

### Vector store — Milvus on Cloudera AI

Vector stores hold **embeddings** (high-dim float vectors that approximate semantic meaning) and let you find the most similar embeddings to a query embedding via **cosine similarity** or other metrics. ARGUS uses **Milvus** running on Cloudera AI as the vector store and **BAAI/bge-base-en-v1.5** as the embedding model.

The corpus has three source types:
- **REGULATION** — chunks of SEBI Master Circular + PFUTP regulations (~1,800 chunks at full corpus)
- **EXEMPLAR** — 200 approved historical STRs (one per JSON file, treated as one chunk each)
- **RULE** — ESM/ASM rule definitions (~150 chunks)

At lab scale, total ~2,200 chunks. Lab 6.1 verifies this loads and that representative manipulation-pattern queries retrieve the right material.

### Embeddings and cosine similarity

When you embed a sentence with BGE-base, the output is a 768-dimensional unit vector. Two sentences with similar meaning produce vectors with high cosine similarity (close to 1.0); unrelated sentences produce low similarity (close to 0.0).

For ARGUS, queries like "layering pattern mid-cap pharma stacked orders cancelled within 200ms" embed to a vector. The nearest regulation chunks (by cosine) should reference PFUTP Reg 4(2)(e) — which prohibits placing orders without intent of execution.

### System prompts — 5 locked constraints

The system prompt is the upfront instruction that constrains every LLM call. ARGUS locks 5 constraints:

1. **NO FABRICATION** — every value in the draft must come from the retrieved corpus or the structured alert payload. If a value isn't available, the field must be `null` (not made up).
2. **NO INVENTED CITATIONS** — only PFUTP regulations actually retrieved from the corpus may be cited.
3. **NO INTENT CLAIM** — describe behavior, not motive. Do not write "the trader intended to manipulate" — write "the orders were cancelled within 200ms with no execution," and let the regulator infer.
4. **NO NAMED CASES** — never reference Jane Street, Adani, or any specific historical case by name. Reference the *pattern* (cross-product manipulation), not the case.
5. **ENGLISH ONLY** — output must be English. Sometimes news context retrieval pulls Hindi; the LLM must not slip into Hindi or any other language.

Lab 6.2 verifies all 5 hold across 5 drafts.

### Why the 5 constraints matter operationally

If the LLM violates Constraint #1 (fabricates a 7.42% price move), the analyst signs the STR, SEBI reads it, finds the number doesn't match exchange tape data — the STR is rejected for *misleading information* and MSE faces a separate compliance violation. The platform makes the problem worse, not better.

If the LLM violates Constraint #3 (claims intent), the legal team rejects the draft because intent is a court determination, not a regulator finding. Every claim of intent extends the drafting time.

The 5 constraints are not academic. They are precisely calibrated to prevent the failure modes that turn a 60-minute task into a 90-minute task.

### Cloudera AI Inference (CAII)

ARGUS uses CAII as the LLM endpoint — a hosted LLM (often Llama-3.1-70B or similar) with OpenAI-compatible chat-completions API. The model itself is provided by your training environment; the lab assumes it's available at `${CAII_ENDPOINT}` with bearer-token auth.

CAII is preferred over external APIs (OpenAI, Anthropic) for surveillance applications because:
1. **Data sovereignty** — alerts contain PII; sending to external APIs may violate DPDP §16.
2. **Predictable latency** — no rate-limiting or quota issues during a SEBI inspection.
3. **Auditability** — CAII logs every call; external API logs vary.

## What Module 6 closes — ARG-4

ARG-4: hand-written STR bottleneck. 40-min drafting × 28 analysts × accumulating backlog = SEBI Show Cause finding.

After Module 6: 8-min review × 28 analysts × no backlog = compliance with the 7-day STR deadline.

## Module 6's labs

| Lab | What you do | Checkpoint | Time |
|---|---|---|---|
| 6.1 — Vector store + RAG retrieval | Build Milvus collection from corpus; verify retrieval quality | CP-15 | ~75 min (most spent on the embedding compute) |
| 6.2 — STR generation | Generate 5 drafts; verify schema, no fabrication, valid citations, no constraint violations | CP-16 | ~90 min |

## Things confusing the first time

### "Why not just train the LLM on the regulatory corpus?"

Three reasons. (1) Training is expensive (compute, time, labeled data). (2) The corpus updates — SEBI publishes new regulations regularly; retraining every time isn't feasible. (3) Retrieval is auditable. RAG lets you say "this draft cited PFUTP Reg 4(2)(e); here's the retrieved chunk; here's the cosine score." Fine-tuned models can't show their work.

### "Why does the embedding model matter? Aren't they all the same?"

No. Embedding models trained on legal/regulatory text retrieve regulatory chunks better than general-purpose models. ARGUS uses BAAI/bge-base-en-v1.5 because it's open-weight (auditable), trained on diverse text including legal, and produces 768-dim vectors that fit in Milvus comfortably. Don't swap the embedding model without re-running CP-15.

### "What if the retrieval finds nothing relevant?"

Then RAG can't help. The fallback in `genai_rag_engine.py` is to mark `narrative_generation_failed = true` and return an error message. The analyst then drafts manually as before. **This is correct behavior** — better to fail loudly than to produce an unsupported draft. Lab 6.2 verifies the fallback works.

### "Drafts include detailed but invented price/volume — what's wrong?"

This is the most common Module 6 failure. LLMs trained on completion-reward prefer "complete" answers over null. Even with Constraint #1, they slot-fill plausible numbers. The fix is an explicit anti-pattern reminder for numeric fields specifically. Lab 6.2 Common Failure Mode covers it.

### "Drafts cite Reg 4(2)(e) for everything"

If every draft cites the same regulation regardless of pattern, retrieval is broken — the embedding model isn't separating manipulation patterns. Re-check Lab 6.1 and the corpus quality (Lab 6.1 Common Failure Mode covers regulation-augmentation).

## Success at end of Module 6

- Build a Milvus vector store from a multi-source regulatory corpus
- Verify retrieval quality with representative manipulation-pattern queries
- Run RAG inference against an LLM endpoint with a constrained system prompt
- Validate LLM output against a structured JSON schema
- Detect and remediate the 5 system-prompt constraint violations
- Operate the entire end-to-end pipeline from alert ID to draft STR

## What's NOT in Module 6

- Training a custom LLM (out of scope; ARGUS uses hosted)
- Fine-tuning the embedding model (out of scope; ARGUS uses BGE pretrained)
- Generating other regulatory artifacts (only STRs; not internal memos)
- Compliance gating (Module 7 — STRs are flagged with SEBI_AUDIT_TRAIL but not erased)

If you find yourself wanting to "automatically file the STR with SEBI" — that's outside the capstone. ARGUS produces drafts; humans approve and file.

---

When ready, head to [Lab 6.1 — Vector Store + RAG Retrieval](../labs/lab-6-1-vector-store.md). Allow ~75 minutes (most of it embedding compute on the corpus).
