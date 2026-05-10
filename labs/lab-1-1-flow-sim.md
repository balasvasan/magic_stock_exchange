# Lab 1.1 — FLOW-SIM Bulk Load (CP-02)

> 👋 **Module 1 first-timer?** Read [`docs/module-1-primer.md`](../docs/module-1-primer.md) before starting this lab. It explains what NiFi, Spark Streaming, PyFlink CEP, SSB SQL are and how the three-engine architecture works. About 20 minutes — well worth the time.

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 (peak volume + real-time detection latency)
> **Time:** ~30 minutes if everything works first try; up to 90 minutes if Kafka or Spark is misbehaving.
> **Source files:** [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py)

## What you're going to do

In order:

1. **Confirm Day 1 outputs are in place** — synthetic data on disk, Kafka topics created. You'll be lost if either is missing, so we check first. (~2 min)
2. **Run FLOW-SIM in oneshot mode** — push the synthetic events into Kafka so NiFi has something to consume in Lab 1.2. (~10–15 min)
3. **Verify partition distribution is even** — confirm one partition isn't getting 100× the traffic of others. (~5 min)
4. **Inspect a single planted manipulation case** — confirm Case 0 (mid-cap pharma layering) is in the Kafka stream end-to-end. (~5 min)
5. **Verify CP-02 pass conditions** — four named checks; all four must pass before moving on. (~5 min)

Total: about 30 minutes of execution + a few minutes of reading and reflection.

## Before you begin — prerequisite checklist

Confirm each of these is true. **If any aren't, fix that first** — chasing this lab's symptoms when the cause is an earlier-step failure is a frustrating way to spend an hour.

- [ ] You completed [Lab 0.1 — Environment Provisioning](lab-0-1-environment-provisioning.md) and CP-00, CP-01 both passed
- [ ] Your `STUDENT_ID` environment variable is set in your current shell — run `echo $STUDENT_ID` and confirm it shows your assigned ID, not blank
- [ ] Your `KAFKA_BROKERS` and `BUCKET_NAME` environment variables are set
- [ ] The synthetic data files exist on disk — run `ls data/generated/ | wc -l` and confirm the count is 14
- [ ] All 11 of your Kafka topics exist — run `kafka-topics --bootstrap-server ${KAFKA_BROKERS} --list | grep -c "argus.${STUDENT_ID}\."` and confirm the count is 11

If any item is missing, return to Lab 0.1 and re-run the relevant steps. Don't try to muscle past — the lab depends on these.

## Why FLOW-SIM exists — read this before Step 2

You might wonder: *we already have data files on disk, and Kafka has a built-in console producer — why do we need a custom Python script to load Kafka?*

Three reasons.

**Reason 1 — One file goes to one topic.** Kafka's console producer takes one file and pushes to one topic. We have 14 files going to 11 topics with specific routing rules: `orders_synthetic.jsonl.gz` goes to `argus.${STUDENT_ID}.orders.v1`, `trades_synthetic.jsonl.gz` to `argus.${STUDENT_ID}.trades.v1`, etc. Doing this by hand means 14 console-producer commands, each with the right partition key, each with the right serializer. FLOW-SIM does this in one command.

**Reason 2 — Partition keying matters.** When Kafka pushes a message to a topic, it picks a partition based on the message *key*. For `orders.v1`, the key is `instrument_code` — that means all orders for a given instrument land on the same partition, which lets a downstream consumer process them in order. The console producer doesn't extract a key from the JSON body. FLOW-SIM does, because the keying behavior is what makes the downstream order-book reconstruction work.

**Reason 3 — Same script does both bulk-load and continuous replay.** FLOW-SIM has two modes: `oneshot` (push everything as fast as Kafka accepts, used today) and `continuous` (push at a controlled rate, used in Lab 1.3 for the 150K ev/s test). Having one script that does both means students learn one tool that covers Day 1 setup *and* Module 1 throughput testing.

The take-away: FLOW-SIM isn't extra complexity — it's a tool that solves a real problem the built-in Kafka utilities don't.

## Step 1 — Confirm Day 1 setup is clean

Quick sanity check before any heavy lifting:

```bash
# Count synthetic data files (expect 14)
ls data/generated/ | wc -l

# Count your Kafka topics (expect 11)
kafka-topics --bootstrap-server ${KAFKA_BROKERS} --list | grep -c "argus.${STUDENT_ID}\."
```

**Expected output:** the first command prints `14`, the second prints `11`.

If either count is wrong, **stop**. Return to [Lab 0.1](lab-0-1-environment-provisioning.md) and re-run the relevant steps. The rest of this lab assumes both numbers are right.

> 💡 **Why these specific numbers?** 14 is the count of generated files (orders, trades, BBO, members, traders, instruments, holidays, news, regulator notices, plus 5 historical-label files). 11 is the count of Kafka topics for your student namespace (9 production + 2 DLQ). Day 1 should have produced exactly these.

## Step 2 — Run FLOW-SIM in oneshot mode

This is the big push. The script reads the JSONL files and pushes their contents into the right Kafka topics with the right partition keys.

```bash
python src/ingest/replay_simulator.py --mode oneshot \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS}
```

**What you should see during the run:** a per-file progress display, with rates in events/sec. Something like this:

```
==> [oneshot] orders_synthetic.jsonl.gz → argus.s001.orders.v1
      ......................................
      [done] 2,500,000 events at 45,000 ev/s
==> [oneshot] trades_synthetic.jsonl.gz → argus.s001.trades.v1
      [done] 175,000 events at 38,000 ev/s
==> [oneshot] bbo_synthetic.jsonl.gz → argus.s001.bbo.v1
      [done] 350,000 events at 42,000 ev/s
==> [oneshot] members_synthetic.jsonl.gz → argus.s001.member.cdc.v1
      [done] 4,500 events at 12,000 ev/s
==> [oneshot] instruments_synthetic.jsonl.gz → argus.s001.instrument.cdc.v1
      [done] 850 events at 8,000 ev/s
... (continues for all 11 topics)
==> oneshot complete: ~3,030,000 events in ~80s
```

(The exact numbers vary slightly with cluster sizing. Numbers in the millions for `orders`, in the thousands for reference data.)

**Expected wall-clock time:** 5–15 minutes depending on cluster size. On a starter sandbox cluster you may see 20–30K ev/s; on a properly-sized CDF cluster you'll see 50–100K ev/s. Either is fine for Day 1 — this is the *bulk-load* test, not the throughput test.

> 💡 **What if it's much slower than 20K ev/s?** Check your `bootstrap-servers` value. If you're hitting a Kafka cluster across a slow network link (e.g., from a local laptop to a cloud cluster) the round-trip time dominates throughput. Run FLOW-SIM from a node *inside* your cluster's VPC if possible.

> ⚠️ **If the script crashes mid-run with `BrokerNotAvailableError`** — Kafka brokers are flaking. See "Common failure mode #2" at the bottom. **Don't re-run the script blindly** — that creates duplicate events.

## Step 3 — Verify Kafka topic distribution

After FLOW-SIM finishes, the topics should have rows. Let's confirm and check that the rows are spread evenly across partitions (not bunched on one).

### Verify each topic has the expected row count

```bash
for t in orders.v1 trades.v1 bbo.v1; do
    full_topic="argus.${STUDENT_ID}.${t}"
    n=$(kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server ${KAFKA_BROKERS} \
        --topic "$full_topic" --time -1 \
        | awk -F: '{sum += $3} END {print sum}')
    echo "$full_topic: $n messages"
done
```

**Expected output** (numbers approximate):

```
argus.s001.orders.v1:  2,500,000
argus.s001.trades.v1:  175,000
argus.s001.bbo.v1:     350,000
```

> 💡 **What `GetOffsetShell` actually does**: it asks Kafka "what's the latest offset for each partition of this topic?" Offsets count from 0. If a topic with 48 partitions has offset 50,000 on each, the topic has 48 × 50,000 = 2,400,000 messages total. The `awk` line sums offsets across partitions.

### Verify partition distribution is even

This is the more interesting check. The orders topic has 48 partitions; we want events spread roughly evenly across them. Heavily skewed distribution (one partition has 100× the rows of others) means the partition key has low cardinality, which would mean the synthetic generator picked a tiny instrument universe — which would mean re-generating the data.

```bash
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{print $3}' | sort -n
```

**Expected output:** 48 numbers, ranging roughly 30,000 to 100,000 each. The highest should be at most ~10× the lowest.

**Looks healthy:**
```
30245
35012
35490
...
89012
95442
```
(Min 30K, max 95K, ratio 3.2× — fine.)

**Looks broken:**
```
112
245
...
89001
523444
```
(Min 112, max 523444, ratio 4,673× — a single instrument is dominating. The synthetic generator made too few instruments.)

If the distribution looks broken, return to Lab 0.1 Step 5 and re-run `data/generate_data.py --seed 42 --out data/generated/`. The seed-42 data is calibrated for healthy distribution.

## Step 4 — Inspect a planted manipulation case

The synthetic generator plants 23 known test cases at fixed indices (0–22). Case 0 is the canonical spoofing-and-layering example: member firm `BNXM-0042` placing rapid-fire NEW + CANCEL orders on a mid-cap pharmaceutical instrument. Modules 2 and 3 will detect this case as a confirmed manipulation pattern. Right now we just want to confirm the case is **in Kafka** end-to-end.

```bash
kafka-console-consumer --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 \
    --from-beginning \
    --max-messages 50000 \
    | grep '"planted_case_idx": 0' \
    | head -3
```

**What this command does:** consume the first 50,000 messages from the topic (taking ~30 seconds), filter to lines containing the planted-case marker, and show the first 3 matches.

**Expected output:** at least 3 JSON lines, each with `"planted_case_idx": 0` and `"member_firm_id": "BNXM-0042"`. The events should look something like:

```json
{"event_id":"...","action":"NEW","member_firm_id":"BNXM-0042","instrument_code":"BNXM-0042-FUT","quantity":50000,"price":...,"planted_case_idx":0,"event_ts":"2026-03-15T11:23:42.117Z"}
{"event_id":"...","action":"CANCEL","member_firm_id":"BNXM-0042",...,"planted_case_idx":0,"event_ts":"2026-03-15T11:23:42.243Z"}
{"event_id":"...","action":"NEW","member_firm_id":"BNXM-0042",...,"planted_case_idx":0,"event_ts":"2026-03-15T11:23:42.358Z"}
```

> 💡 **What you're seeing:** the spoofer puts a large order in (NEW), waits ~125 ms, cancels it (CANCEL), waits ~115 ms, places another (NEW). The same pattern repeats roughly 5–10 times across the planted case window. Module 1's PyFlink CEP job (you'll deploy it in Lab 1.4) is what catches this pattern in real time.

If grep returns 0 lines, the synthetic data wasn't generated with `--seed 42` — the planted cases live at deterministic offsets and seeded data is required. Re-run `data/generate_data.py --seed 42` and reload Kafka.

## Step 5 — Verify CP-02 pass conditions

CP-02 has **four checks**. All four must pass.

### Check 1 — All three streaming topics have non-zero traffic

```bash
for t in orders.v1 trades.v1 bbo.v1; do
    n=$(kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server ${KAFKA_BROKERS} \
        --topic "argus.${STUDENT_ID}.${t}" --time -1 \
        | awk -F: '{sum += $3} END {print sum}')
    echo "argus.${STUDENT_ID}.${t}: $n"
done
```

**Pass if:** all three counts > 100,000. **Fail if:** any count is 0 or near-zero.

### Check 2 — Partition distribution is even on `orders.v1`

```bash
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{print $3}' | sort -n \
    | awk 'NR==1 {min=$1} END {max=$1; print "min:", min, "max:", max, "ratio:", max/min}'
```

**Pass if:** ratio < 10. **Fail if:** ratio ≥ 10.

### Check 3 — Planted Case 0 is visible in the stream

```bash
kafka-console-consumer --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 \
    --from-beginning --max-messages 50000 \
    | grep -c '"planted_case_idx": 0'
```

**Pass if:** count ≥ 3. **Fail if:** count is 0.

### Check 4 — DLQ topics are empty

```bash
for dlq in orders.dlq trades.dlq; do
    n=$(kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server ${KAFKA_BROKERS} \
        --topic "argus.${STUDENT_ID}.${dlq}" --time -1 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}')
    echo "argus.${STUDENT_ID}.${dlq}: $n"
done
```

**Pass if:** both DLQ counts are 0. **Fail if:** either DLQ has messages — that means FLOW-SIM emitted malformed records, which usually means a corrupted data file. Re-run `data/generate_data.py --seed 42`.

---

## Common failure mode #1 — Kafka rejects messages with `RecordTooLargeException`

**Symptom:** FLOW-SIM stops mid-run with:
```
kafka.errors.RecordTooLargeError: The message is N bytes when serialized which is larger than the maximum request size
```

**Cause:** A planted-case event (typically Case 3, momentum-ignition) has a very large `book_state_after` JSON body that exceeds the broker's `max.request.size` (default 1 MB).

**Diagnosis** — confirm by checking the topic's per-message size:
```bash
kafka-topics --bootstrap-server ${KAFKA_BROKERS} \
    --describe --topic argus.${STUDENT_ID}.orders.v1
```
Look for the `max.message.bytes` config; if it's < 5 MB, you've found the cause.

**Fix:** bump the producer config in `src/ingest/replay_simulator.py`:
```python
producer = KafkaProducer(
    ...
    max_request_size=10 * 1024 * 1024,  # 10 MiB
    ...
)
```
Also make sure the broker side allows it — your instructor may need to update the topic-level `max.message.bytes`.

**Prevention next time:** generate data with default `--scale 0.05`. Higher scale produces fatter `book_state_after` payloads.

## Common failure mode #2 — `BrokerNotAvailableError` partway through the run

**Symptom:** FLOW-SIM crashes after pushing some files successfully:
```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

**Cause:** Either a broker actually crashed (rare) or your `KAFKA_BROKERS` env var has a typo (common).

**Diagnosis:**
```bash
echo $KAFKA_BROKERS
# Should show something like: kafka1.cluster:9092,kafka2.cluster:9092,kafka3.cluster:9092
# If it shows blank or only 1 broker, that's the bug
```

If only one broker is listed and it goes down, you have no failover. Lab clusters should have ≥ 3 brokers.

**Fix:** ask your instructor for the correct comma-separated broker list, set it, **then re-run FLOW-SIM**. Important: re-running won't create duplicates because the topics will already have the previously-pushed messages. Kafka topics accumulate; FLOW-SIM doesn't dedupe. If you want a clean reset, your instructor will need to delete and recreate the topics. **For Day 1, don't do that** — just continue past the duplicates.

## Common failure mode #3 — All checks pass except partition distribution is skewed

**Symptom:** Check 2 reports `ratio: 247` (or similar large number).

**Cause:** Synthetic data was generated with too few distinct `instrument_code` values, so the partition key collapses onto a few partitions.

**Diagnosis:**
```bash
zcat data/generated/instruments_synthetic.jsonl.gz | wc -l
# Should be ~ 800-1000
```
If it's < 200, the generator didn't produce a realistic instrument universe.

**Fix:** re-run the data generator with the canonical seed:
```bash
rm -rf data/generated/
python data/generate_data.py --seed 42 --out data/generated/
# Then re-run FLOW-SIM oneshot
```

---

## Pass condition for CP-02

All four checks above pass. When they do:

- ✅ Kafka has roughly 3 million events spread across the streaming topics
- ✅ Events are distributed evenly across partitions (no hot keys)
- ✅ Planted Case 0 is identifiable in the stream
- ✅ DLQ topics are empty (no malformed records)

You're ready for Lab 1.2, where the four NiFi flows and four Spark Streaming jobs will be deployed to consume from these topics and write Bronze.

## Wrap-up — what you can now do that you couldn't before

You can now load synthetic event data into a real Kafka cluster, partitioned by business key, in a single command. You can verify a Kafka topic by reading offsets, confirming distribution, and inspecting individual messages for known patterns. **You've also done your first manipulation-pattern inspection** — you know what a planted spoofing case looks like as raw JSON in Kafka, before any platform job touches it. That's a skill the surveillance team uses every day when investigating real cases.

When you're ready, head to [Lab 1.2 — Bronze Ingest Deployment](lab-1-2-bronze-ingest.md). Allow about 60 minutes for that one — it's the longest lab in Module 1.
