# Module 2 — Identity Resolution + Order Book Reconstruction

Days 3-4 · Closes **ARG-2 (Part 1)** · CP-05 (Iceberg time-travel) · CP-06 (fuzzy match)

## Two parallel pipelines

```mermaid
flowchart LR
    classDef bronze  fill:#1e2535,stroke:#9ca3af,color:#e5e7eb
    classDef step    fill:#161b27,stroke:#f96302,color:#f96302
    classDef silver  fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px
    classDef gold    fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px
    classDef cp      fill:#1a1632,stroke:#6366f1,color:#6366f1,stroke-width:2px

    subgraph P1["JOB-05 — Identity Resolution"]
      direction LR
      I1[(bronze.member_cdc)]:::bronze
      F["Fuzzy match<br/>PAN edit-dist ≤ 1<br/>Name similarity ≥ 0.85<br/>Demat prefix"]:::step
      M["SCD2 MERGE<br/>canonical entity_id<br/>known_aliases array"]:::step
      O1[(silver.member_master)]:::silver
      I1 --> F --> M --> O1
    end

    subgraph P2["JOB-06 — Order Book Reconstruction"]
      direction LR
      I2[("bronze.orders_raw<br/>silver.member_master<br/>silver.instrument_master")]:::bronze
      E["Enrich with<br/>member category +<br/>instrument metadata"]:::step
      W["Window aggregate<br/>1S / 100MS / PER_EVENT"]:::step
      O2[(gold.order_book_snapshots)]:::gold
      I2 --> E --> W --> O2
    end

    O1 -.feeds into.-> I2

    O1 --> CP6["✓ CP-06<br/>Cases 10-14 (fuzzy)<br/>resolve to single entity_id<br/>known_aliases ≥ 2"]:::cp
    O2 --> CP5["✓ CP-05<br/>FOR SYSTEM_VERSION AS OF<br/>reproduces book at any T<br/>incl. planted Case 0"]:::cp
```

## What this closes

The legacy platform couldn't:
- **Reconstruct the book at past timestamps** — investigators waited 11 weeks for offline reconstructions that were too late to enforce
- **Merge investors across brokers** — coordinated multi-broker manipulation looked like 3+ independent participants

Module 2 fixes both. Iceberg time-travel makes book reconstruction a single SQL query (`FOR SYSTEM_VERSION AS OF <snapshot>`); multi-signal fuzzy match merges Cases 10-14 in JOB-05.

## The time-travel test

```sql
-- CP-05: reproduce the book state when planted Case 0 was happening
SELECT *
FROM argus_${STUDENT_ID}_gold.order_book_snapshots
  FOR SYSTEM_VERSION AS OF <pre-event-snapshot-id>
WHERE instrument_code = 'BNXM-0042-FUT' AND
      snapshot_ts BETWEEN '2026-03-15 11:23:42.000' AND '2026-03-15 11:23:42.500';
-- Should show 5+ stacked layered orders cancelled within 200ms of an opposite-side fill
```
