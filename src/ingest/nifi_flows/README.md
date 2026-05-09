# NiFi flow exports

Four NiFi flows that feed the Bronze layer. These are reference exports — students will *import* them into their own NiFi canvas in Lab 1.2, then customize them.

| Flow file | Purpose | Feeds Spark job |
|---|---|---|
| [`flow_01_orders_landing.json`](flow_01_orders_landing.json) | Mirrors TARANG multicast → `argus.orders.v1` Kafka topic | JOB-01 |
| [`flow_02_trades_landing.json`](flow_02_trades_landing.json) | NIPATAN clearing producer → `argus.trades.v1` Kafka topic | JOB-02 |
| [`flow_03_kavach_cdc.json`](flow_03_kavach_cdc.json) | Debezium → KAVACH CDC → `argus.member.cdc.v1` (compacted) | JOB-03 |
| [`flow_04_external_feeds.json`](flow_04_external_feeds.json) | SEBI SI Portal SFTP + BBO vendor feed + news wire → external Kafka topics | JOB-04 |

## Why both NiFi *and* Spark Structured Streaming?

NiFi is the **landing-zone orchestrator** — the layer responsible for talking to source systems, validating record-level structure, applying lightweight transformations (decompression, format conversion, enrichment with a static lookup, schema versioning), and routing failures to a DLQ. NiFi is the right fit when you have many heterogeneous sources, schema drift, retry-with-backoff requirements, and operations staff who want a visual flow they can debug at runtime.

Spark Structured Streaming is the **Bronze-table writer** — the layer responsible for stateful, exactly-once writes into Iceberg, schema-enforcement at the column level, partition routing, and late-data handling.

Splitting responsibilities this way is the standard CDF + CDE pattern: NiFi handles the messy world outside the data platform; Spark handles the rigorous world inside it.

## Why the JSON files are stubs in the repo

The actual NiFi flow exports are large (10–50 KB each) and contain UUIDs and processor positions that are environment-specific. The reference exports ship with the lab; students download them from `s3://argus-training-assets/argus-capstone/v1.0/nifi_flows/` during Lab 1.1 and import via the NiFi REST API. The files in this directory are **placeholders** so that documentation references resolve and so that the repo structure is complete.

For the lab, see [`labs/lab-1-2-bronze-ingest.md`](../../../labs/lab-1-2-bronze-ingest.md).
