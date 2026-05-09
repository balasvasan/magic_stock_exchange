# Lab 1.1 — FLOW-SIM Oneshot Bulk Load (CP-02)

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 (peak volume crisis)
> **Source files:** [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py)

## Objectives

- Understand why FLOW-SIM exists — why we don't just run a Kafka console producer
- Bulk-load the synthetic events into Kafka so NiFi has something to consume
- Verify all 11 Kafka topics receive traffic at the expected partition distribution
- Inspect a single planted manipulation case end-to-end in Kafka before any Spark job touches it

## Why FLOW-SIM exists

NiFi flows need *live* Kafka traffic to demonstrate backpressure handling, DLQ routing, schema-validation failures, and rate-driven autoscaling decisions. There are three obvious alternatives, all worse:

- **A direct producer from `data/generated/`**: writes events as fast as the producer's buffer drains. Doesn't simulate realistic timing — events bunch up at start of session and trail off, instead of producing a steady-state firehose.
- **A console producer**: works for one topic at a time, no rate control, no key extraction, no awareness of which file feeds which topic. Fine for ad-hoc testing; useless for a 10-day capstone.
- **Replaying real exchange data**: not legally available, and even if it were, the ML training labels (analyst dispositions on confirmed manipulation) are MSE proprietary.

FLOW-SIM solves all three: it routes JSONL files to the right Kafka topics with the right partition keys, supports both bulk-load (Day 1) and rate-limited replay (Module 1 throughput tests), and leaves the planted test cases at known indices for downstream verification.

## Procedure

### Step 1 — Confirm Day 1 setup is clean

```bash
ls data/generated/ | wc -l                    # expect 14
kafka-topics --bootstrap-server ${KAFKA_BROKERS} --list | grep -c argus.   # expect 8
```

If either count is wrong, see [Lab 0.1](lab-0-1-environment-provisioning.md) before continuing.

### Step 2 — Run FLOW-SIM in oneshot mode

```bash
python src/ingest/replay_simulator.py --mode oneshot \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS}
```

**What you should see**: the script processes three files (`orders_synthetic.jsonl.gz`, `trades_synthetic.jsonl.gz`, `bbo_synthetic.jsonl.gz`) and reports per-file event counts and rates. At `--scale 0.05` data (the Day 1 default), expect roughly:

```
==> [oneshot] orders_synthetic.jsonl.gz → argus.${STUDENT_ID}.orders.v1
      ...
      [done] 2,500,000 events at 45,000 ev/s
==> [oneshot] trades_synthetic.jsonl.gz → argus.${STUDENT_ID}.trades.v1
      [done] 175,000 events at 38,000 ev/s
==> [oneshot] bbo_synthetic.jsonl.gz → argus.${STUDENT_ID}.bbo.v1
      [done] 350,000 events at 42,000 ev/s
==> oneshot complete: 3,025,000 events in 75.3s (40,200 ev/s)
```

(Numbers will vary slightly with cluster sizing.)

> 💡 **Tip:** Oneshot mode pushes as fast as Kafka will accept. On a small dev cluster you may see 20K–30K ev/s; on a properly sized production-grade CDF cluster you should hit 50K–100K ev/s sustained. Module 1 CP-03 demands 150K — that's the *continuous* mode test, not this one.

### Step 3 — Verify Kafka topic distribution

In SMM (Streams Messaging Manager) navigate to your CDF cluster and confirm the partition message counts for `argus.${STUDENT_ID}.orders.v1`. They should be roughly **even** across all 48 partitions — variation under ~10× between min and max partition is healthy. Skewed distribution (one partition with 10× the message count of others) means the partition key (`instrument_code`) has very low cardinality, which would mean the synthetic generator picked a tiny instrument universe — re-check `data/generated/instruments.csv` row count.

```bash
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{print $3}' | sort -n | uniq -c | head
```

### Step 4 — Inspect a planted case in Kafka

Confirm Case 0 (mid-cap pharma layering by member firm BNXM-0042) is present in the order stream:

```bash
kafka-console-consumer --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --from-beginning --max-messages 50000 \
    | grep '"planted_case_idx": 0' \
    | head -3
```

You should see at least three JSON events tagged `"planted_case_idx": 0` with `"member_firm_id": "BNXM-0042"` and rapid-fire NEW + CANCEL actions. These are the layered orders that Modules 2 and 3 will detect as a confirmed manipulation pattern.

## Checkpoint CP-02 — NiFi/Kafka ingest healthy

### Pass condition

Run all four checks; all four must pass.

### Check 1 — All three streaming topics have traffic

```bash
for t in argus.${STUDENT_ID}.orders.v1 argus.${STUDENT_ID}.trades.v1 argus.${STUDENT_ID}.bbo.v1; do
    n=$(kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server ${KAFKA_BROKERS} --topic "$t" --time -1 \
        | awk -F: '{sum += $3} END {print sum}')
    echo "$t: $n messages"
done
```

**Expected output**: each topic shows a non-zero message count. At `--scale 0.05`:
- `argus.${STUDENT_ID}.orders.v1`: ~2,500,000
- `argus.${STUDENT_ID}.trades.v1`: ~175,000
- `argus.${STUDENT_ID}.bbo.v1`: ~350,000

### Check 2 — Partition distribution is even

```bash
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{print $3}' | sort -n
```

**Expected output**: 48 partition counts. The maximum-to-minimum ratio should be < 10× (e.g. min ~30k, max ~100k is fine; min 1k max 500k means key skew).

### Check 3 — Planted Case 0 is in the stream

The grep in Step 4 above returns ≥ 3 lines. If it returns 0 lines, the synthetic generator wasn't run with `--seed 42` — re-generate.

### Check 4 — DLQ is empty

```bash
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} --topic argus.${STUDENT_ID}.orders.dlq --time -1 2>&1 | head
```

**Expected output**: either "Topic does not exist" (the DLQ topic is created on first failure by JOB-01) or a count of 0 across all partitions.

If DLQ is non-empty at this stage, FLOW-SIM is producing malformed records — most likely cause is a corrupted `orders_synthetic.jsonl.gz` (re-run `data/generate_data.py`).

---

## Common failure mode — Kafka rejects messages with `RecordTooLargeException`

**Symptom**: FLOW-SIM stops mid-run with `kafka.errors.RecordTooLargeError: The message is N bytes when serialized which is larger than the maximum request size`.

**Diagnosis**: a planted-case event (typically Case 3, momentum-ignition) has a very large `book_state_after` JSON body that exceeds the default `max.request.size = 1 MB`. This shouldn't happen with the default generator, but it can happen if `--scale` is set very high and `book_state_after` is enriched with too much depth.

**Fix**: bump the producer config in `replay_simulator.py`:

```python
producer = KafkaProducer(
    ...
    max_request_size=10 * 1024 * 1024,  # 10 MiB
    ...
)
```

Also make sure the broker side allows it — the topic-level `max.message.bytes` must be ≥ what the producer sends.

---

## Pass condition for CP-02

All four checks pass. Once they do, you have a Kafka cluster full of realistic order traffic, and you're ready to deploy the NiFi flows + Spark Structured Streaming jobs in [Lab 1.2](lab-1-2-bronze-ingest.md).
