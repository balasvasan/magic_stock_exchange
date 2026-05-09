# Lab 6.2 — STR Generation (CP-16)

> ℹ️ **Module:** 6 — GenAI STR Narrative Engine
> **Closes deficiency:** ARG-4 (the central narrative-drafting bottleneck)
> **Source files:** [`src/ml/genai_rag_engine.py`](../src/ml/genai_rag_engine.py), [`src/ml/system_prompts.py`](../src/ml/system_prompts.py)

## Objectives

- Generate STR drafts for the five real planted manipulation cases (0, 1, 2, 4, 5)
- Verify each draft conforms to the required JSON schema
- Confirm no draft fabricates a price, volume, member name, or trader ID not in the alert payload
- Confirm each draft cites a specific PFUTP regulation appropriate to the pattern type
- Confirm none of the drafts violates the four other locked system-prompt constraints

## Why this matters

This is the operational test of ARG-4's fix. If the drafts are well-formed, well-cited, and faithful to the alert payload, the surveillance team's 60-minute drafting task collapses to an 8-minute review. If the drafts hallucinate prices or assert intent the analyst can't sign off on, every draft becomes a 90-minute task instead of 60 — the platform makes the problem *worse*. The five locked system-prompt constraints exist precisely to prevent that failure.

## Procedure

### Step 1 — Confirm prerequisites

```python
import os
assert os.environ.get("CAII_ENDPOINT"), "Set CAII_ENDPOINT to your CAII LLM URL"
assert os.environ.get("CAII_TOKEN"),    "Set CAII_TOKEN to your CAII auth token"
```

If your environment uses a different auth mechanism (mTLS, Kerberos), update the `call_llm` function in `genai_rag_engine.py` accordingly. The reference implementation uses Bearer token auth, which is the CAII default for OpenAI-compatible endpoints.

Confirm Lab 6.1's vector store is loaded:

```python
from pymilvus import connections, Collection
connections.connect("default", host="${MILVUS_HOST}", port="19530")
coll = Collection("argus_${STUDENT_ID}_str_corpus"); coll.load()
print(f"Collection size: {coll.num_entities}")
# expect: > 2,000 (CP-15 pass threshold)
```

### Step 2 — Find the alert IDs for the 5 real planted cases

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

Pick the most recent alert for each member firm. Note 5 alert IDs (you'll need them for the STR generation calls).

### Step 3 — Generate 5 STR drafts

For each of the 5 alert IDs, run:

```bash
python src/ml/genai_rag_engine.py \
    --alert-id <ALERT_ID> \
    --milvus-host ${MILVUS_HOST} \
    --llm-endpoint ${CAII_ENDPOINT} \
    > drafts/<ALERT_ID>.json
```

**Expected output per call**: a JSON object printed to stdout with the schema:

```json
{
  "alert_id": "...",
  "str_id": "STR-AB12CD34EF",
  "executive_summary": "...",
  "order_flow_narrative": "...",
  "quantified_market_impact": {
    "price_move_pct": ...,
    "volume_during_window": ...,
    "retail_account_exposure_inr": ...
  },
  "suspected_violation_citation": "PFUTP Reg 4(2)(e)",
  "recommended_next_steps": "...",
  "narrative_generation_failed": false,
  "model_endpoint": "...",
  "drafted_at": "..."
}
```

Each call should complete in 8–20 seconds end-to-end. Save the 5 JSON files in `drafts/`.

### Step 4 — Validate each draft against the schema

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
    if not (40 <= len(doc.get("executive_summary", "").split()) <= 80):
        # PRD says max 60 words; allow a small tolerance
        if len(doc["executive_summary"].split()) > 80:
            failures.append((path.name, f"executive_summary too long: {len(doc['executive_summary'].split())} words"))

print(f"Drafts validated: {len(list(Path('drafts').glob('*.json')))}")
print(f"Failures: {len(failures)}")
for f in failures:
    print(f"  {f}")
```

**Expected output**: 5 drafts, 0 failures.

### Step 5 — Verify no fabrication

For each draft, the values in `quantified_market_impact` must come from the alert payload, not be invented by the LLM:

```python
# Pull each alert's structured payload and compare to its draft
for path in Path("drafts").glob("*.json"):
    draft = json.loads(path.read_text())
    alert_id = draft["alert_id"]
    # Pull the alert from Spark (replace with your spark session)
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

**Expected output**: 5 ✓ marks. If any draft references an instrument or member firm that wasn't in the alert payload, the LLM has hallucinated — that's a constraint #1 (NO FABRICATION) violation and the draft must be discarded.

### Step 6 — Verify PFUTP regulation citations are appropriate

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

**Expected output**: 5 ✓ marks. If any citation doesn't match the pattern type, the LLM picked a regulation from the retrieved context that doesn't fit — review the retrieval results for that alert and consider broadening the regulatory corpus.

### Step 7 — Verify no constraint violations in narrative

Spot-check each `order_flow_narrative`:

- **Constraint #3 (no intent claim)**: search for the strings `"intended to"`, `"the trader's intent"`, `"in order to"`. The narrative should describe behavior, not impute intent.
- **Constraint #4 (no named cases)**: search for `"Jane Street"`, `"similar to the"`, `"comparable to"`. The narrative should not reference any historical case by name.
- **Constraint #5 (English output)**: confirm the entire narrative is English (no Hindi or other-language slips from news context).

```python
forbidden = ["intended to", "the trader intended", "Jane Street", "similar to the case"]
for path in Path("drafts").glob("*.json"):
    draft = json.loads(path.read_text())
    text = draft["order_flow_narrative"].lower()
    for phrase in forbidden:
        if phrase.lower() in text:
            print(f"  ⚠ {path.name}: forbidden phrase '{phrase}' present")
```

**Expected output**: zero violations. If any phrase is found, the LLM is bypassing the system prompt — the most common cause is a stale system prompt cached in the CAII deployment; restart the LLM service to re-read the constraints.

## Checkpoint CP-16 — STR drafts are valid, faithful, and compliant

### Pass condition

All five checks pass.

### Check 1 — 5 drafts produced

`drafts/` contains 5 JSON files corresponding to the 5 real planted cases.

### Check 2 — All 5 drafts conform to the schema

Step 4 reports 0 failures.

### Check 3 — No fabrication

Step 5 reports 5 ✓ marks. Every narrative references the actual instrument and member firm from the payload.

### Check 4 — All 5 PFUTP citations match the pattern type

Step 6 reports 5 ✓ marks.

### Check 5 — No system-prompt constraint violations

Step 7 reports 0 forbidden phrases.

---

## Common failure mode — drafts include detailed but invented price/volume figures

**Symptom**: a draft for Case 0 includes a `quantified_market_impact.price_move_pct` of 7.42 — but the alert payload has no `price_move_pct` field. The LLM is filling the slot with a plausible-looking number.

**Diagnosis**: the JSON schema in the system prompt allows `null` for any of the quantified-impact fields, but LLMs frequently prefer numeric over null because the training distribution rewards "complete" answers. The system prompt's constraint #1 says "if a value is not in the payload, write [not provided]" — which it follows for prose fields but ignores for structured numeric fields.

**Fix**: add an explicit anti-pattern reminder to the system prompt for numeric fields. In `src/ml/system_prompts.py`, append to constraint #1:

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

This pattern — LLMs slot-fill plausible-looking numbers — is the single most dangerous failure mode for surveillance applications, because the fabricated numbers look authoritative when copied into an STR. The defense is the system prompt's explicit instruction to prefer null, repeatedly and unambiguously.

---

## Pass condition for CP-16

All five checks pass. The 60-minute hand-drafted STR collapses to an 8-minute review of an LLM-generated draft. ARG-4 — the documentation backlog that defined a separate finding in SEBI's Show Cause Notice — is operationally closed.
