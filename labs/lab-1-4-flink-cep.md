# Lab 1.4 — PyFlink CEP Real-Time Pattern Detection (CP-02b)

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
>
> ⏱ **Time budget:** 90 minutes (Day 3 morning)
>
> ✅ **Closes:** ARG-1 (peak detection latency, not just ingest throughput)

## What this lab teaches

You'll deploy your first Flink streaming application — JOB-10, a PyFlink CEP job that detects R-101 SPOOFING and R-102 LAYERING patterns on the live order stream. Unlike the Spark batch rules in JOB-08 (Module 3), this job fires within **800ms p99** of the triggering event.

This is your introduction to **complex event processing** — the discipline of detecting *patterns of events over time* rather than facts in a database. CEP is what production exchanges use because spoofing is fundamentally a temporal pattern: you can't tell whether an order was spoofing until you see it cancelled, with an opposite-side fill following close behind.

## Prerequisites

- [ ] Lab 1.2 complete — your Bronze ingest jobs are running and `argus.${STUDENT_ID}.orders.v1` is receiving traffic from FLOW-SIM
- [ ] You can submit Flink jobs to your CDP cluster (your instructor will give you the Flink CLI gateway or SSB UI URL)
- [ ] PyFlink installed locally (`pip install apache-flink==1.17.1` if running locally — production uses cluster install)

## Key concepts before you start

### What is CEP?

**CEP (Complex Event Processing)** detects sequences/patterns/correlations across event streams. The Flink CEP library lets you write patterns like:

> *"event A, followed within 200ms by event B where B's instrument matches A's, then within 100ms by event C with C.side != A.side"*

...as code. Flink handles the windowing, state management, watermark alignment, and parallelism. You handle the pattern logic.

### Why not just use Spark Structured Streaming?

Spark Structured Streaming is excellent for "transform/aggregate windowed streams." But it isn't built for *event-pattern matching*. To detect "A followed by B within 200ms" in Spark you'd self-join the stream against itself with timestamp arithmetic — slow, awkward, and doesn't scale. Flink CEP makes the same query a 5-line declarative pattern.

The CEP path also gives you **sub-second detection** because it's optimized for low-latency event-time processing. Spark Structured Streaming's micro-batch model fundamentally trades latency for throughput.

### The pattern we're implementing

```python
Pattern.begin("large_resting_cancel")
    .where(lambda e: e["action"] == "CANCEL" and e["qty"] >= 5000)
    .next("opposite_fill")
    .where(lambda c, ctx: (
        c["action"] == "FILL"
        and c["instrument_code"] == ctx.get_events_for_pattern("large_resting_cancel")[0]["instrument_code"]
        and c["side"] != ctx.get_events_for_pattern("large_resting_cancel")[0]["side"]
    ))
    .within(Time.milliseconds(200))
```

Read it left to right: "begin watching for a CANCEL event of qty ≥ 5000; immediately after, watch for a FILL on the same instrument but opposite side, within 200ms of the cancel." If both happen, fire a SPOOFING alert.

This is exactly the R-101 spoofing pattern — and it's exactly what JOB-08 batch checks every 30 minutes via SQL. The difference is latency.

## Step 1 — Read the job code

Open `src/ingest/flink_cep/job_10_realtime_spoofing_layering.py` in your editor. Take 10 minutes to read through it. Don't worry if you don't understand every Flink API — focus on these:

- The `parse_event` function: how Kafka JSON becomes a dict
- `build_spoofing_pattern()` and `build_layering_pattern()`: the actual CEP patterns
- `emit_spoof_alert` / `emit_layer_alert`: how matched events become alert JSON for `argus.${STUDENT_ID}.realtime_alerts.v1`
- `main()`: source → keyed-by-instrument → CEP detect → sink

Notice that `consumer_group()`, `topic()`, and `cde_job()` come from `src/common/naming.py` — same per-student namespacing as everywhere else.

## Step 2 — Submit the job to Flink

The exact submission command depends on your CDP environment. Common patterns:

### CDP with Cloudera Streaming Analytics (CSA)

```bash
# From a CSA gateway node (your instructor will give you the host)
flink run -py src/ingest/flink_cep/job_10_realtime_spoofing_layering.py \
    -pyfs src/common/naming.py \
    --bootstrap "${KAFKA_BROKERS}" \
    --parallelism 4
```

### Cloudera Manager UI

1. Cloudera Manager → Flink → "Submit Job"
2. Browse to your local copy of `job_10_realtime_spoofing_layering.py`
3. Add `src/common/naming.py` as a Python files dependency
4. Set Args: `--bootstrap ${KAFKA_BROKERS} --parallelism 4`
5. Submit

After submission, verify the job is running:

```bash
flink list -r | grep "argus_${STUDENT_ID}_realtime_cep"
```

You should see one running job with that name.

## Step 3 — Generate test traffic

If FLOW-SIM continuous mode isn't already running from Lab 1.3, start it now:

```bash
python src/ingest/replay_simulator.py \
    --mode continuous \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS} \
    --rate 10000 --duration 300
```

(Lower rate is fine for this lab — we're testing detection, not throughput.)

## Step 4 — Verify CP-02b pass condition (the latency test)

> ⚡ **Critical for CP-02b:** the test isn't "did the alert fire" — it's "did it fire fast enough?"

While FLOW-SIM is running, watch the realtime alerts topic:

```bash
kafka-console-consumer \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.realtime_alerts.v1 \
    --from-beginning \
    --max-messages 20 \
    --property print.timestamp=true \
    --formatter "kafka.tools.DefaultMessageFormatter" \
    --property "key.separator= | " \
    | head -20
```

You should see alerts streaming in. Each alert's JSON contains a `detection_latency_ms` field — that's the time from the cancel event (the trigger) to the alert being emitted.

### CP-02b pass condition

Compute p99 detection latency over 100 alerts:

```bash
kafka-console-consumer \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.realtime_alerts.v1 \
    --from-beginning --max-messages 100 \
    | jq -r '.detection_latency_ms' \
    | sort -n | awk 'BEGIN{c=0} {a[c++]=$1} END{print "p99 =", a[int(c*0.99)], "ms"}'
```

**Pass condition**: p99 < 800ms. Typical successful runs see 200-400ms.

### If your p99 is too high

| Symptom | Likely cause | Fix |
|---|---|---|
| p99 > 5000ms | Parallelism too low for partition count | Re-submit with `--parallelism 8` |
| Latency spikes every ~10s | Checkpoint barrier alignment stalls | Increase `--checkpoint-interval-ms 30000` |
| Some alerts have huge latency | Kafka consumer lag building up | Check FLOW-SIM rate; reduce if needed |

## Step 5 — Cross-check with batch (preview of Module 3)

After your Flink job has been running for a while, the SAME planted Case 0 spoofing event will eventually appear in `gold.alert_candidates` once JOB-08 batch runs in Module 3 (Day 6).

The lab in Module 3 will revisit this — for now just verify the row is in the realtime stream:

```sql
SELECT alert_id, source_engine, rule_id, pattern_type,
       member_firm_id, instrument_code, detection_latency_ms
FROM argus_${STUDENT_ID}_gold.realtime_alert_stream
WHERE pattern_type = 'SPOOFING'
ORDER BY fired_ts DESC
LIMIT 5;
```

(Note: this requires JOB-12 from `src/ingest/job_12_realtime_alert_persistence.py` to also be running — typically your instructor starts this once and leaves it; if rows aren't appearing, ask.)

## Lab summary

You've now deployed a streaming pattern-detection engine that catches spoofing in real-time. The same pattern is also implemented in JOB-08 batch — Module 3 covers that. Lab 1.5 will compare them side-by-side.

The lesson: **streaming and batch detection complement each other**. Streaming catches it fast (analyst can intervene); batch catches it with full context (legal proof). Neither replaces the other in production surveillance.

## Common errors

**`No module named 'pyflink'`** — PyFlink not installed in your environment. On a CSA gateway it should be pre-installed; locally do `pip install apache-flink==1.17.1` (matches Cloudera CSA 1.10's Flink version).

**`Topic argus.s001.realtime_alerts.v1 not found`** — re-run `bash sql/provision_environment.sh` to create the new topic. The topic was added in v1.2 of the PRD.

**`State backend exceeded heap size`** — your TTL settings aren't kicking in. Check that the job is using keyed state (the `.key_by(lambda e: e["instrument_code"])` line in `main()`). If so, increase the TaskManager heap (cluster admin task).

**Alerts firing on legitimate orders** — the threshold (`SPOOF_MIN_QTY = 5_000`) is calibrated for the synthetic data. Real production exchanges tune this empirically.
