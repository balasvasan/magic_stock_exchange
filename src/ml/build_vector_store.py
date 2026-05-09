#!/usr/bin/env python3
"""
build_vector_store — Milvus ingest for the STR drafter (Module 6 setup)
=======================================================================
One-time / monthly job. Loads three corpora into a Milvus collection
that the GenAI engine queries at STR-drafting time:

  1. Regulatory corpus  — SEBI Master Circular on Surveillance of
                          Securities Market (09-Jul-2024) + PFUTP
                          Regulations 2003, paragraph-chunked.
  2. Exemplar STRs      — ~200 historical STR narratives approved by
                          Compliance and accepted by SEBI.
  3. ESM/ASM rules      — current ESM and ASM rule definitions.

The news corpus is NOT pre-loaded — news is fetched at runtime per alert
because it changes constantly and the relevant window is ±24h around
each alert.

Embedding model: locally hosted multilingual BGE variant on Cloudera AI
Inference (CAII) — required for DPDP data residency.

PRD reference: §9 (GenAI component).
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# These imports raise informatively if the lab environment doesn't have them
try:
    from pymilvus import (
        Collection, CollectionSchema, DataType, FieldSchema,
        MilvusClient, connections, utility,
    )
except ImportError as exc:  # pragma: no cover
    print("ERROR: pymilvus not installed. Run: pip install pymilvus", file=sys.stderr)
    raise SystemExit(1) from exc

try:
    from sentence_transformers import SentenceTransformer
except ImportError as exc:  # pragma: no cover
    print("ERROR: sentence-transformers not installed. Run: pip install sentence-transformers",
          file=sys.stderr)
    raise SystemExit(1) from exc

from src.common.naming import milvus_collection


EMBEDDING_DIM   = 768   # BGE-base-en-v1.5
CHUNK_SIZE      = 800   # characters per chunk (≈ 200 tokens)
CHUNK_OVERLAP   = 100   # overlap between adjacent chunks


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP
               ) -> Iterable[str]:
    """Naive sliding-window chunker. Production deployments use a
    paragraph-aware splitter; the simple version is fine for SEBI text
    which is already paragraph-numbered."""
    text = " ".join(text.split())
    if len(text) <= size:
        yield text
        return
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        yield text[start:end]
        if end == len(text):
            return
        start += (size - overlap)


def ensure_collection(client: MilvusClient, collection_name: str) -> None:
    """Drop + recreate the collection. Always start fresh on full rebuild."""
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)

    schema = CollectionSchema(fields=[
        FieldSchema("chunk_id",      DataType.VARCHAR, max_length=64,  is_primary=True),
        FieldSchema("source_type",   DataType.VARCHAR, max_length=32),  # REGULATION | EXEMPLAR | RULE
        FieldSchema("source_doc",    DataType.VARCHAR, max_length=256),
        FieldSchema("section_ref",   DataType.VARCHAR, max_length=128),
        FieldSchema("text",          DataType.VARCHAR, max_length=4000),
        FieldSchema("embedding",     DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
    ])
    Collection(name=collection_name, schema=schema)
    coll = Collection(collection_name)
    coll.create_index(field_name="embedding",
                      index_params={"index_type": "HNSW",
                                    "metric_type": "COSINE",
                                    "params": {"M": 16, "efConstruction": 200}})
    coll.load()
    print(f"==> Collection {collection_name} created (dim={EMBEDDING_DIM}, HNSW/COSINE)")


def embed_batch(model: SentenceTransformer, texts: list[str]) -> list[list[float]]:
    """Embed a batch; sentence-transformers handles batching internally."""
    return model.encode(texts, normalize_embeddings=True, show_progress_bar=False).tolist()


def insert_chunks(client: MilvusClient, model: SentenceTransformer,
                  rows: list[dict], collection_name: str,
                  batch_size: int = 64) -> int:
    """Embed and insert chunks into Milvus. Returns count inserted."""
    coll = Collection(collection_name)
    inserted = 0
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start: batch_start + batch_size]
        embs = embed_batch(model, [r["text"] for r in batch])
        coll.insert([
            [r["chunk_id"]    for r in batch],
            [r["source_type"] for r in batch],
            [r["source_doc"]  for r in batch],
            [r["section_ref"] for r in batch],
            [r["text"]        for r in batch],
            embs,
        ])
        inserted += len(batch)
    coll.flush()
    return inserted


def load_regulation_corpus(corpus_dir: Path) -> list[dict]:
    """Load SEBI Master Circular + PFUTP from corpus_dir/regulations/.

    Each .txt file contains one document; section_ref is parsed from the
    filename, e.g. 'pfutp_reg_4_2_e.txt' → 'PFUTP Reg 4(2)(e)'.
    """
    rows = []
    reg_dir = corpus_dir / "regulations"
    if not reg_dir.is_dir():
        print(f"  [warn] {reg_dir} not found; skipping regulations")
        return rows
    for path in sorted(reg_dir.glob("*.txt")):
        section_ref = path.stem.replace("_", " ").upper()
        text = path.read_text(encoding="utf-8")
        for chunk_idx, chunk in enumerate(chunk_text(text)):
            rows.append({
                "chunk_id":    f"reg-{path.stem}-{chunk_idx:03d}",
                "source_type": "REGULATION",
                "source_doc":  path.name,
                "section_ref": section_ref,
                "text":        chunk,
            })
    return rows


def load_exemplar_strs(corpus_dir: Path) -> list[dict]:
    """Load 200 exemplar STRs from corpus_dir/exemplars/*.json.

    Each JSON has the same shape as the LLM output schema, so the model
    learns the format at retrieval time.
    """
    rows = []
    ex_dir = corpus_dir / "exemplars"
    if not ex_dir.is_dir():
        print(f"  [warn] {ex_dir} not found; skipping exemplars")
        return rows
    for path in sorted(ex_dir.glob("*.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"  [warn] skipping malformed exemplar {path.name}: {exc}")
            continue
        # Embed the executive_summary + order_flow_narrative as searchable text.
        searchable = f"{doc.get('executive_summary', '')}\n\n{doc.get('order_flow_narrative', '')}"
        section_ref = doc.get("suspected_violation_citation", "PFUTP unspecified")
        rows.append({
            "chunk_id":    f"exemplar-{path.stem}",
            "source_type": "EXEMPLAR",
            "source_doc":  path.name,
            "section_ref": section_ref,
            "text":        searchable[:3900],  # truncate to varchar limit
        })
    return rows


def load_rules_corpus(corpus_dir: Path) -> list[dict]:
    """ESM/ASM rule definitions from corpus_dir/rules/*.md."""
    rows = []
    rules_dir = corpus_dir / "rules"
    if not rules_dir.is_dir():
        return rows
    for path in sorted(rules_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        for chunk_idx, chunk in enumerate(chunk_text(text)):
            rows.append({
                "chunk_id":    f"rule-{path.stem}-{chunk_idx:03d}",
                "source_type": "RULE",
                "source_doc":  path.name,
                "section_ref": path.stem,
                "text":        chunk,
            })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--milvus-host", default="localhost")
    parser.add_argument("--milvus-port", default="19530")
    parser.add_argument("--corpus-dir", type=Path,
                        default=Path("data/genai_corpus"),
                        help="Root dir with regulations/, exemplars/, rules/")
    parser.add_argument("--embedding-model",
                        default="BAAI/bge-base-en-v1.5",
                        help="sentence-transformers model name")
    parser.add_argument("--rebuild", action="store_true",
                        help="Drop existing collection and rebuild from scratch")
    args = parser.parse_args()

    print(f"==> Connecting to Milvus at {args.milvus_host}:{args.milvus_port}")
    connections.connect("default", host=args.milvus_host, port=args.milvus_port)
    client = MilvusClient(uri=f"http://{args.milvus_host}:{args.milvus_port}")

    collection_name = milvus_collection("str_corpus")
    print(f"==> Target collection: {collection_name}")

    print(f"==> Loading embedding model: {args.embedding_model}")
    model = SentenceTransformer(args.embedding_model)

    if args.rebuild or not utility.has_collection(collection_name):
        ensure_collection(client, collection_name)

    print(f"==> Loading regulation corpus from {args.corpus_dir}/regulations")
    reg_rows = load_regulation_corpus(args.corpus_dir)
    print(f"    {len(reg_rows):>5,} regulation chunks")

    print(f"==> Loading exemplar STRs from {args.corpus_dir}/exemplars")
    ex_rows = load_exemplar_strs(args.corpus_dir)
    print(f"    {len(ex_rows):>5,} exemplar STRs")

    print(f"==> Loading rules from {args.corpus_dir}/rules")
    rule_rows = load_rules_corpus(args.corpus_dir)
    print(f"    {len(rule_rows):>5,} rule chunks")

    all_rows = reg_rows + ex_rows + rule_rows
    if not all_rows:
        print("ERROR: no corpus content found; did you create data/genai_corpus/?",
              file=sys.stderr)
        return 1

    print(f"==> Embedding + inserting {len(all_rows):,} chunks into {collection_name}")
    n = insert_chunks(client, model, all_rows, collection_name)
    print(f"==> Inserted {n:,} chunks. Collection ready for queries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
