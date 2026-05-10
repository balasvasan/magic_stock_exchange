# Lab 6.2 — STR Generation (CP-16)

> 👋 **Module 6 first-timer?** Read [`docs/module-6-primer.md`](../docs/module-6-primer.md) first. About 20 minutes — the 5 system-prompt constraints are explained there.

> ℹ️ **Module:** 6 — GenAI STR Narrative Engine
> **Closes deficiency:** ARG-4 (the central narrative-drafting bottleneck)
> **Time:** ~90 minutes if all 5 drafts pass first try; up to 4 hours if constraints are violated and the system prompt needs tuning.
> **Source files:** [`src/ml/genai_rag_engine.py`](../src/ml/genai_rag_engine.py), [`src/ml/system_prompts.py`](../src/ml/system_prompts.py)

## What you're going to do

1. **Confirm prerequisites** — Lab 6.1 vector store loaded, CAII endpoint accessible. (~3 min)
2. **Find alert IDs for the 5 real planted cases** (0, 1, 2, 4, 5). (~5 min)
3. **Generate 5 STR drafts** by running the RAG pipeline per alert. (~30 min — most is LLM inference)
4. **Validate each draft against the JSON schema.** (~10 min)
5. **Verify no fabrication** — instrument and member firm referenced literally. (~10 min)
6. **Verify PFUTP citations match pattern types.** (~10 min)
7. **Verify no system-prompt constraint violations** in narratives. (~10 min)
8. **Verify CP-16 pass conditions** — five named checks. (~10 min)

Total: ~90 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 6.1](lab-6-1-vector-store.md) is complete and CP-15 passed; collection is loadable
- [ ] CAII endpoint is set: `echo $CAII_ENDPOINT` returns a URL; `echo $CAII_TOKEN` returns a token
- [ ] You have `gold.alert_candidates` populated (Module 3) for the 5 planted real-case member firms
- [ ] `mkdir -p drafts` (you'll save the 5 JSON drafts here)

## Why this lab is the operational test of ARG-4 — read this before Step 3

This is the operational test of the entire Module 6 thesis. **If the drafts are well-formed, well-cited, and faithful to the alert payload, the surveillance team's 60-minute drafting task collapses to an 8-minute review.** Every analyst hour saved is recovered capacity for the team.

**If the drafts hallucinate prices, claim intent the analyst can't sign off on, or cite invented regulations, every draft becomes a 90-minute task instead of 60 — the platform makes the problem *worse*.** The 5 locked system-prompt constraints exist precisely to prevent that failure.

The 5 constraints, recapped:
1. **NO FABRICATION** — every value must come from the retrieved corpus or alert payload.
2. **NO INVENTED CITATIONS** — only retrieved PFUTP regulations may be cited.
3. **NO INTENT CLAIM** — describe behavior, not motive.
4. **NO NAMED CASES** — never reference Jane Street, Adani, etc. by name.
5. **ENGLISH ONLY** — never slip into Hindi or another language.

Lab 6.2 verifies all 5 hold across 5 drafts.

## Step 1 — Confirm prerequisites

```python
import os
assert os.environ.get("CAII_ENDPOINT"), "Set CAII_ENDPOINT to your CAII LLM URL"
assert os.environ.get("CAII_TOKEN"),    "Set CAII_TOKEN to your CAII auth token"
print(f"CAII endpoint: {os.environ['CAII_ENDPOINT']}")
```

If your environment uses a different auth mechanism (mTLS, Kerberos), update the `call_llm` function in `genai_rag_engine.py` accordingly. The reference implementation uses Bearer token auth, which is the CAII default for OpenAI-compatible endpoints.

Confirm Lab 6.1's vector store is loaded:

```python
from pymilvus import connections, Collection
connections.connect("default", host=os.environ["MILVUS_HOST"], port="19530")
coll = Collection("argus_${STUDENT_ID}_str_corpus"); coll.load()
print(f"Collection size: {coll.num_entities}")
```
**Expected:** > 2,000 (CP-15 pass threshold).

## Step 2 — Find alert IDs for the 5 real planted cases

```sql
-- Locate one alert per real planted case (member firms 0-5 from PRD §11)
SELECT a.alert_id, a.member_firm_id, a.pattern_type, a.severity, a.fired_ts
FROM argus_${STUDENT_ID}_gold.alert_candidates a
WHERE a.member_firm_id IN (
    'BNXM-0042',  -- case 0 (layering)
    'BNXM-0117',  -- case 1 (spoofing) and case 9 (multi-day)
    'BNXM-0231',  -- case 2 (marking-the-close)
    'BNXM-0089',  -- case 3 (momentum ignition)
    'BNXM-0276'   -- case 5 (wash trade)
)
  AND a.disposition = 'PENDING'
ORDER BY a.member_firm_id, a.fired_ts DESC;
```

Pick the **most recent** alert for each member firm. Note the 5 alert IDs.

> 💡 **Why pick the most recent?** A member firm typically has multiple alerts (the same pattern can fire across multiple instruments or days). The most recent gives you the freshest planted-case signal. Older alerts from the same firm may have been generated against earlier feature snapshots before the pattern was fully present in the data.

## Step 3 — Generate 5 STR drafts

For each of the 5 alert IDs:

```bash
mkdir -p drafts/
python src/ml/genai_rag_engine.py \
    --alert-id <ALERT_ID> \
    --milvus-host ${MILVUS_HOST} \
    --llm-endpoint ${CAII_ENDPOINT} \
    > drafts/<ALERT_ID>.json
```

> 💡 **What `genai_rag_engine.py` does:** loads the alert's structured payload from `gold.alert_candidates`, embeds the pattern description as a query, retrieves top-k regulatory chunks + similar exemplars from Milvus, builds the system prompt with the 5 constraints, calls the CAII LLM endpoint, parses + validates the JSON output, returns the validated STR. End-to-end takes 8–20 seconds per alert depending on LLM latency.

**Expected output per call** (printed JSON):

```json
{
  "alert_id": "...",
  "str_id": "STR-AB12CD34EF",
  "executive_summary": "...",
  "order_flow_narrative": "...",
  "quantified_market_impact": {
    "price_move_pct": 7.42,
    "volume_during_window": 1842300,
    "retail_account_exposure_inr": 12473921
  },
  "suspected_violation_citation": "PFUTP Reg 4(2)(e)",
  "recommended_next_steps": "...",
  "narrative_generation_failed": false,
  "model_endpoint": "...",
  "drafted_at": "..."
}
```

Save the 5 JSON files in `drafts/`.

If any call returns `"narrative_generation_failed": true`, see Common Failure Mode #1.

## Step 4 — Validate each draft against the schema

```python
import json
from pathlib import Path

REQUIRED_KEYS = {"alert_id", "str_id", "executive_summary", "order_flow_narrative",
                 "quantified_market_impact", "suspected_violation_citation",
                 "recommended_next_steps", "narrative_generation_failed"}

failures = []
for path in Path("drafts").glob("*.json"):
    doc = json.loads(path.read_text())
    missing = REQUIRED_KEYS - set(doc.keys())
    if missing:
        failures.append((path.name, f"missing keys: {missing}"))
    if doc.get("narrative_generation_failed"):
        failures.append((path.name, "fallback fired (LLM failed)"))
    word_count = len(doc.get("executive_summary", "").split())
    if word_count > 80:
        failures.append((path.name, f"executive_summary too long: {word_count} words"))

print(f"Drafts validated: {len(list(Path('drafts').glob('*.json')))}")
print(f"Failures: {len(failures)}")
for f in failures:
    print(f"  {f}")
```

**Expected:** 5 drafts, 0 failures.

> 💡 **Why 80 words for executive_summary?** PRD specifies 60 words max, but LLMs sometimes overshoot. We allow a small tolerance (80) to avoid spurious failures. If the summary is 100+ words, the constraint is being ignored — the system prompt's word-limit instruction needs to be more explicit.

## Step 5 — Verify no fabrication

For each draft, the values in `quantified_market_impact` must come from the alert payload, **not** be invented by the LLM:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

for path in Path("drafts").glob("*.json"):
    draft = json.loads(path.read_text())
    alert_id = draft["alert_id"]
    alert = spark.sql(f"SELECT instrument_code, member_firm_id, "
                      f"window_start_ts, features "
                      f"FROM argus_${STUDENT_ID}_gold.alert_candidates "
                      f"WHERE alert_id = '{alert_id}'").collect()[0]
    instrument = alert["instrument_code"]
    member = alert["member_firm_id"]
    # The draft narrative must reference the actual instrument and member
    assert instrument in draft["order_flow_narrative"], \
        f"{path.name}: instrument {instrument} not mentioned in narrative"
    assert member in draft["order_flow_narrative"], \
        f"{path.name}: member {member} not mentioned in narrative"
    print(f"  ✓ {path.name}: instrument + member referenced literally")
```

**Expected:** 5 ✓ marks. If any draft references an instrument or member firm that wasn't in the alert payload, the LLM has hallucinated — that's a Constraint #1 (NO FABRICATION) violation; the draft must be discarded.

> 💡 **Why this specific test?** If the LLM gets the instrument or member firm name wrong, every other field in the draft is suspect. The instrument and member are the *anchor* — if they're right, you can spot-check other facts; if they're wrong, the draft is unusable. The check is therefore a cheap proxy for overall faithfulness.

If any check fails, see Common Failure Mode #2.

## Step 6 — Verify PFUTP regulation citations match pattern types

```python
expected_regs = {
    "LAYERING":            "4(2)(e)",
    "SPOOFING":            "4(2)(e)",
    "MARKING_THE_CLOSE":   "4(2)(g)",
    "MOMENTUM_IGNITION":   "4(2)(e)",
    "WASH":                "4(2)(a)",
    "CROSS_PRODUCT":       "4(2)(e)",   # or 4(2)(g) — both acceptable
}

for path in Path("drafts").glob("*.json"):
    draft = json.loads(path.read_text())
    alert = spark.sql(f"SELECT pattern_type FROM argus_${STUDENT_ID}_gold.alert_candidates "
                      f"WHERE alert_id = '{draft['alert_id']}'").collect()[0]
    pattern = alert["pattern_type"]
    citation = draft["suspected_violation_citation"]
    expected = expected_regs.get(pattern, "")
    if expected and expected not in citation:
        print(f"  ⚠ {path.name}: pattern={pattern}, expected '{expected}' in citation, got '{citation}'")
    else:
        print(f"  ✓ {path.name}: pattern={pattern} cites {citation}")
```

**Expected:** 5 ✓ marks. If any citation doesn't match the pattern type, the LLM picked a regulation from the retrieved context that doesn't fit. Most likely cause: retrieval returned a regulation that's textually similar but operationally inappropriate.

If any check fails, see Common Failure Mode #3.

## Step 7 — Verify no system-prompt constraint violations

Spot-check each `order_flow_narrative` for forbidden phrases:

```python
forbidden = [
    "intended to",                  # Constraint #3 (no intent claim)
    "the trader intended",          # Constraint #3
    "in order to manipulate",       # Constraint #3
    "Jane Street",                  # Constraint #4 (no named cases)
    "similar to the case",          # Constraint #4
    "comparable to",                # Constraint #4 (often)
]

violations = 0
for path in Path("drafts").glob("*.json"):
    draft = json.loads(path.read_text())
    text = draft["order_flow_narrative"].lower()
    for phrase in forbidden:
        if phrase.lower() in text:
            print(f"  ⚠ {path.name}: forbidden phrase '{phrase}' present")
            violations += 1

print(f"Total violations: {violations}")
```

**Expected:** zero violations.

If any phrase is found, the LLM is bypassing the system prompt — the most common cause is a stale system prompt cached in the CAII deployment. See Common Failure Mode #4.

> 💡 **Constraint #5 (English only) check:** if you see Devanagari script (`अ-ह`), Bengali (`অ-হ`), or any other non-English script in any narrative, that's a Constraint #5 violation. Practically rare with modern LLMs but worth a quick eyeball pass.

## Step 8 — Verify CP-16 pass conditions

CP-16 has **five checks**.

### Check 1 — 5 drafts produced

`drafts/` contains 5 JSON files corresponding to the 5 real planted cases. **Pass if:** 5 files. **Fail if:** fewer.

### Check 2 — All 5 drafts conform to the schema

Step 4 reports 0 failures. **Pass if:** 0 failures. **Fail if:** > 0.

### Check 3 — No fabrication

Step 5 reports 5 ✓ marks. Every narrative references the actual instrument and member firm. **Pass if:** all 5. **Fail if:** any miss.

### Check 4 — All 5 PFUTP citations match pattern type

Step 6 reports 5 ✓ marks. **Pass if:** all 5. **Fail if:** any mismatch.

### Check 5 — No system-prompt constraint violations

Step 7 reports 0 forbidden phrases. **Pass if:** 0. **Fail if:** > 0.

---

## Common failure mode #1 — Drafts include detailed but invented price/volume figures

**Symptom:** a draft for Case 0 includes a `quantified_market_impact.price_move_pct` of 7.42 — but the alert payload has no `price_move_pct` field. The LLM is filling the slot with a plausible-looking number.

**Cause:** the JSON schema in the system prompt allows `null` for any of the quantified-impact fields, but **LLMs frequently prefer numeric over null because the training distribution rewards "complete" answers.** The system prompt's Constraint #1 says "if a value is not in the payload, write [not provided]" — which it follows for prose fields but ignores for structured numeric fields.

**Fix:** add an explicit anti-pattern reminder to the system prompt for numeric fields. In `src/ml/system_prompts.py`, append to Constraint #1:

```python
SYSTEM_PROMPT = SYSTEM_PROMPT + """

EXTRA REMINDER FOR NUMERIC FIELDS:
For quantified_market_impact, ANY field that lacks a corresponding value in the
structured payload MUST be set to null in the JSON. Do NOT estimate, infer, or
extrapolate price moves, volumes, or rupee figures. Null is the correct value
when data is absent. A null is far better than a plausible-but-fabricated number.
"""
```

Re-run the 5 drafts. The numeric fields should now be `null` for any value not in the payload.

> 💡 **This is the single most dangerous failure mode for surveillance applications,** because the fabricated numbers look authoritative when copied into an STR. The defense is the system prompt's explicit instruction to prefer null, repeatedly and unambiguously.

## Common failure mode #2 — `narrative_generation_failed = true`

**Symptom:** Step 3 returns a JSON with `"narrative_generation_failed": true` and an error message instead of a draft.

**Cause** (in decreasing likelihood):
1. **CAII endpoint timeout.** LLM took > 30s to respond.
2. **Retrieval returned no chunks** for this alert's pattern. RAG fallback fires.
3. **JSON validation failed** — LLM returned malformed output.

**Diagnosis:** check the error message in the failed JSON's `error` field. The error message identifies which case applies.

**Fix:**
1. **Timeout:** retry; if persistent, increase the timeout in `genai_rag_engine.py`'s `call_llm` (default 30s → 60s).
2. **No chunks:** Lab 6.1 retrieval is broken. Re-run Lab 6.1 Step 4 for this pattern; if it fails too, fix the corpus (Lab 6.1 Common Failure Mode #2).
3. **JSON malformed:** the LLM produced text instead of JSON. Add `"Respond with JSON only. No prose before or after."` to the system prompt as a final reminder.

## Common failure mode #3 — Citation doesn't match pattern type

**Symptom:** Step 6 shows a CRITICAL alert for marking-the-close pattern but the citation is `4(2)(e)` instead of `4(2)(g)`.

**Cause:** retrieval returned `4(2)(e)` chunks at higher cosine score than `4(2)(g)` because the corpus has more `4(2)(e)` material. The LLM trusted the retrieval ranking.

**Fix:** add more `4(2)(g)` material to `data/genai_corpus/regulations/`. Specifically, augmented chunks for marking-the-close patterns. Re-run Lab 6.1's `--rebuild`.

Alternatively, edit the system prompt to instruct: "When the alert's pattern_type is MARKING_THE_CLOSE, prefer PFUTP Reg 4(2)(g) over 4(2)(e) even if retrieval ranks 4(2)(e) higher." This is more brittle but doesn't require corpus changes.

## Common failure mode #4 — Forbidden phrases keep appearing

**Symptom:** Step 7 reports forbidden phrases on multiple drafts. You re-deploy the system prompt; phrases still appear.

**Cause:** CAII may be caching the prompt template. Modifications to `system_prompts.py` aren't picked up until the LLM service is restarted.

**Fix:**
```bash
# Restart the CAII deployment (instructor handles)
# Or, if you have access:
cml-cli inference restart --endpoint ${CAII_ENDPOINT_NAME}
```

Wait 60 seconds, re-run drafts, re-test.

## Common failure mode #5 — Some drafts pass, others fail randomly

**Symptom:** out of 5 drafts, 3 pass cleanly, 2 have constraint violations. Re-running gives different drafts that fail in different ways.

**Cause:** **LLM nondeterminism**. Default temperature isn't 0; runs vary. ARGUS sets `temperature=0` in `genai_rag_engine.py`, but if you've modified the call params, it may have leaked.

**Fix:** ensure `temperature=0` and `top_p=1.0` in the LLM call. Re-run drafts.

For full determinism in a regulatory context, use `temperature=0, top_p=1, seed=42`. The CAII chat-completions API supports `seed`; setting it makes the LLM deterministic across runs (assuming no model-side updates).

---

## Pass condition for CP-16

All five checks pass:
- ✅ 5 drafts produced
- ✅ All 5 drafts conform to the schema
- ✅ No fabrication (all reference actual instrument + member)
- ✅ All 5 PFUTP citations match pattern type
- ✅ No system-prompt constraint violations

When all five pass, the **60-minute hand-drafted STR collapses to an 8-minute review of an LLM-generated draft**. ARG-4 — the documentation backlog that defined a separate finding in SEBI's Show Cause Notice — is operationally closed.

## Wrap-up — what you can now do that you couldn't before

You can run an end-to-end RAG pipeline that drafts a structured regulatory artifact (an STR) grounded in retrieved corpus material. You can validate LLM output against a JSON schema, against payload-faithfulness, against citation appropriateness, and against system-prompt constraint violations. You can diagnose and remediate the 5 most common LLM failure modes for regulatory text generation.

**Module 6 is complete.** And that's also the last module of the capstone. Module 7 (CP-17, CP-18, CP-19) was completed in your earlier work — `_${STUDENT_ID}` Atlas tags, consent withdrawal, and the COMPLIANCE GATE erasure with Iceberg time-travel proof.

When all six checkpoints (CP-15, CP-16, plus all the others from Modules 1–5 and Module 7) pass, you have built and verified a complete next-generation surveillance platform. ARGUS closes ARG-1 through ARG-5, and the SEBI Show Cause Notice findings — from the 14 missed manipulation episodes to the 7-day STR backlog — are systematically addressed.

You should review the [final capstone wrap-up](../docs/wrap-up.md) for the consolidated retrospective and what a real production deployment would add on top of the lab build.
