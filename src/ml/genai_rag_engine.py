#!/usr/bin/env python3
"""
genai_rag_engine — STR narrative drafter (Module 6)
====================================================
Closes ARG-4: 40-minute hand-written STR narratives → 8-minute LLM-drafted
+ analyst-reviewed.

PRD reference: §9.

Pipeline (per the PRD locked spec):
    1. Pull confirmed-alert structured payload from gold.alert_candidates
    2. Retrieve top-5 relevant SEBI/PFUTP citations from Milvus
    3. Retrieve top-3 most similar historical exemplar STRs from Milvus
    4. Fetch news headlines for the instrument over ±24h alert window
    5. Assemble grounded prompt using system_prompts.GROUNDING_TEMPLATE
    6. Call CAII-hosted LLM endpoint
    7. Parse response into structured JSON; validate against schema
    8. Write draft to gold.alert_candidates.str_id and a sidecar
       JSON object (the analyst UI reads from there)

Fallback behavior (PRD §9):
    - Malformed JSON  → retry once with stricter prompt
    - Still malformed → write FALLBACK_TEMPLATE skeleton with
                        narrative_generation_failed=True
    - Never silently fail; every confirmed alert always gets *something*.

The LLM endpoint is a CAII-hosted open-weights model (Llama 3.1 70B or
Mistral Large per PRD §9). DPDP §16 + RBI guidance require local hosting
for personal data of Indian data principals; do not call out to OpenAI
or Anthropic from this engine.

Resource names (Milvus collection, table refs, app name) resolved from
src.common.naming using ${STUDENT_ID}.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer

from system_prompts import (
    FALLBACK_TEMPLATE, GROUNDING_TEMPLATE, SYSTEM_PROMPT,
)

from src.common.naming import fqtn, milvus_collection, cde_job

LOG = logging.getLogger("argus.genai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TOP_K_REGULATIONS = 5
TOP_K_EXEMPLARS   = 3
NEWS_WINDOW_HOURS = 24
LLM_TIMEOUT_SEC   = 30
REQUIRED_KEYS = {
    "executive_summary", "order_flow_narrative", "quantified_market_impact",
    "suspected_violation_citation", "recommended_next_steps",
}


@dataclass
class STRDraft:
    """Output schema. The dataclass mirrors the locked JSON shape."""
    alert_id: str
    str_id: str
    executive_summary: str
    order_flow_narrative: str
    quantified_market_impact: dict
    suspected_violation_citation: str
    recommended_next_steps: str
    narrative_generation_failed: bool = False
    model_endpoint: str = ""
    drafted_at: str = ""


def retrieve(coll: Collection, embedder: SentenceTransformer,
             query: str, source_type: str, top_k: int) -> list[dict]:
    """Vector search over Milvus, scoped to source_type."""
    qvec = embedder.encode([query], normalize_embeddings=True).tolist()
    hits = coll.search(
        data=qvec, anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"ef": 64}},
        limit=top_k,
        expr=f'source_type == "{source_type}"',
        output_fields=["source_doc", "section_ref", "text"],
    )
    return [{"section_ref": h.entity.get("section_ref"),
             "text":         h.entity.get("text"),
             "source_doc":   h.entity.get("source_doc"),
             "score":        float(h.score)}
            for h in hits[0]]


def fetch_news_for_instrument(spark, instrument_code: str,
                              window_start_ts: str, window_hours: int = 24
                              ) -> list[str]:
    """Pull news headlines from argus_silver tables for the alert window
    (+/- window_hours). Real production reads from a dedicated news service;
    in the lab we read from the synthetic news Bronze table."""
    sql = f"""
        SELECT headline FROM {fqtn("bronze", "external_feeds")}
        WHERE source = 'NEWS'
          AND instrument_code = '{instrument_code}'
          AND event_ts BETWEEN
              TIMESTAMP '{window_start_ts}' - INTERVAL {window_hours} HOURS
              AND TIMESTAMP '{window_start_ts}' + INTERVAL {window_hours} HOURS
        ORDER BY event_ts DESC LIMIT 10
    """
    try:
        rows = spark.sql(sql).collect()
        return [r["headline"] for r in rows if r["headline"]]
    except Exception as exc:                                          # pragma: no cover
        LOG.warning("News fetch failed for %s: %s", instrument_code, exc)
        return []


def call_llm(endpoint: str, system: str, user: str,
             max_tokens: int = 1500, temperature: float = 0.1) -> str:
    """POST to a CAII-hosted OpenAI-compatible chat endpoint.
    The model is configured via the endpoint URL — the engine is
    intentionally model-agnostic so we can swap Llama/Mistral/etc."""
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }
    headers = {"Authorization": f"Bearer {os.environ.get('CAII_TOKEN', '')}",
               "Content-Type":  "application/json"}
    resp = requests.post(endpoint, json=payload, headers=headers,
                         timeout=LLM_TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def validate_json(raw: str) -> dict | None:
    """Parse + schema-validate the LLM output. Returns dict on success, None on failure."""
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(doc, dict):
        return None
    if not REQUIRED_KEYS.issubset(doc.keys()):
        return None
    qmi = doc.get("quantified_market_impact")
    if not isinstance(qmi, dict):
        return None
    return doc


def draft_str(alert: dict, regulations: list[dict], exemplars: list[dict],
              news: list[str], llm_endpoint: str) -> STRDraft:
    """Run the RAG pipeline for one confirmed alert."""
    grounding = GROUNDING_TEMPLATE.format(
        alert_id=alert["alert_id"],
        pattern_type=alert.get("pattern_type", "[not provided]"),
        member_firm_id=alert["member_firm_id"],
        member_firm_name=alert.get("member_firm_name", "[not provided]"),
        member_firm_category=alert.get("member_firm_category", "[not provided]"),
        trader_id=alert.get("trader_id", "[not provided]"),
        instrument_code=alert["instrument_code"],
        instrument_type=alert.get("instrument_type", "[not provided]"),
        sector=alert.get("sector", "[not provided]"),
        underlying_code=alert.get("underlying_code", "[not provided]"),
        window_start_ts=alert["window_start_ts"],
        window_end_ts=alert["window_end_ts"],
        severity=alert.get("severity", "[not provided]"),
        model_score=alert.get("model_score", "[not provided]"),
        features_block=json.dumps(alert.get("features_dict", {}), indent=2),
        regulation_context="\n\n".join(
            f"[{r['section_ref']}] {r['text']}" for r in regulations),
        exemplars_block="\n\n---\n\n".join(
            f"Exemplar ({e['section_ref']}):\n{e['text']}" for e in exemplars),
        news_block="\n".join(f"- {h}" for h in news) if news else "[none in window]",
    )

    # First attempt
    raw = call_llm(llm_endpoint, SYSTEM_PROMPT, grounding)
    parsed = validate_json(raw)

    # Retry once with a stricter reminder if the first attempt was malformed
    if parsed is None:
        LOG.warning("Alert %s: first LLM call returned malformed JSON; retrying",
                    alert["alert_id"])
        stricter = SYSTEM_PROMPT + (
            "\n\nIMPORTANT: your previous response was not valid JSON. "
            "Return ONLY a single JSON object matching the schema. "
            "No preamble, no markdown, no commentary.")
        raw = call_llm(llm_endpoint, stricter, grounding)
        parsed = validate_json(raw)

    # Fallback if both attempts failed
    if parsed is None:
        LOG.error("Alert %s: LLM failed twice; using fallback template",
                  alert["alert_id"])
        parsed = {
            k: (v.format(**alert) if isinstance(v, str) else v)
            for k, v in FALLBACK_TEMPLATE.items()
        }

    return STRDraft(
        alert_id=alert["alert_id"],
        str_id=f"STR-{uuid.uuid4().hex[:10].upper()}",
        executive_summary=parsed["executive_summary"],
        order_flow_narrative=parsed["order_flow_narrative"],
        quantified_market_impact=parsed["quantified_market_impact"],
        suspected_violation_citation=parsed["suspected_violation_citation"],
        recommended_next_steps=parsed["recommended_next_steps"],
        narrative_generation_failed=bool(parsed.get("narrative_generation_failed", False)),
        model_endpoint=llm_endpoint,
        drafted_at="<set by caller>",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--alert-id", required=True,
                        help="Specific alert_id to draft an STR for")
    parser.add_argument("--llm-endpoint",
                        default=os.environ.get("CAII_ENDPOINT",
                                               "http://caii-llm:8000/v1/chat/completions"))
    parser.add_argument("--milvus-host", default="localhost")
    parser.add_argument("--milvus-port", default="19530")
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(cde_job("ml.genai_rag_engine")).getOrCreate()

    alerts_table = fqtn("gold", "alert_candidates")
    LOG.info("Loading alert %s from %s", args.alert_id, alerts_table)
    df = spark.sql(f"SELECT * FROM {alerts_table} "
                   f"WHERE alert_id = '{args.alert_id}'").toPandas()
    if df.empty:
        LOG.error("Alert %s not found", args.alert_id)
        return 1
    alert = df.iloc[0].to_dict()
    if isinstance(alert.get("features"), str):
        try:
            alert["features_dict"] = json.loads(alert["features"])
        except json.JSONDecodeError:
            alert["features_dict"] = {}

    LOG.info("Connecting to Milvus + loading embedding model")
    connections.connect("default", host=args.milvus_host, port=args.milvus_port)
    collection_name = milvus_collection("str_corpus")
    coll = Collection(collection_name); coll.load()
    embedder = SentenceTransformer(args.embedding_model)

    query = (f"{alert.get('pattern_type', '')} "
             f"{alert.get('instrument_type', '')} "
             f"manipulation surveillance Indian exchange")
    regulations = retrieve(coll, embedder, query, "REGULATION", TOP_K_REGULATIONS)
    exemplars   = retrieve(coll, embedder, query, "EXEMPLAR",   TOP_K_EXEMPLARS)
    news        = fetch_news_for_instrument(
        spark, alert["instrument_code"],
        alert["window_start_ts"].isoformat() if hasattr(alert["window_start_ts"], "isoformat")
        else str(alert["window_start_ts"]),
        NEWS_WINDOW_HOURS)

    LOG.info("Retrieved %d regulations, %d exemplars, %d news headlines",
             len(regulations), len(exemplars), len(news))

    draft = draft_str(alert, regulations, exemplars, news, args.llm_endpoint)
    print(json.dumps(asdict(draft), indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
