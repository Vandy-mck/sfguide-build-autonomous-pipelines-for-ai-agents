# Openflow — Kafka Connector

This folder contains the Openflow connector definition for ingesting TMS data from Kafka/Redpanda into Snowflake.

## Overview

The connector (`Openflow_Kafka_SUMMIT_DE238.json`) is a pre-built Openflow flow definition that:

1. Consumes JSON messages from Kafka/Redpanda topics using regex-based topic matching
2. Authenticates via SASL (SCRAM-SHA-256)
3. Maps topic names to Snowflake table names using an expression-based processor
4. Writes data into the `RAW` schema using Snowflake-managed authentication

## Prerequisites

- Openflow deployment provisioned (see `1_bootstrap/setup.sql` — `SUMMIT_DEPLOYMENT`)
- Openflow runtime created and running (see `2_dcm_project/scripts/post_deploy.sql`)
- Kafka/Redpanda topics exist with data flowing (see `3_generate/README.md`)
- `SUMMIT_INGEST_ROLE_DEV` with write access to `SUMMIT_DB_DEV.RAW`
- External access integration `SUMMIT_EAI` configured for Kafka broker egress

## Recommended approach: Import from Registry

The preferred way to create the connector is via the Openflow Registry:

1. Using the `SUMMIT_ADMIN` role, open the Openflow runtime in Snowsight
2. In the canvas, drag-and-drop **Import from Registry**
3. Select **kafka-json-sasl-topic2table-schemaev** and click Import
4. Configure the 3 parameter contexts (see below)
5. Set the topic-to-table mapping expression
6. Enable controller services and start the flow

## Fallback: Import from file

If you encounter configuration problems with the registry flow, import the pre-configured flow file:

1. In the Openflow canvas, drag-and-drop a new **Processor Group**
2. Choose **Import from file** and upload `Openflow_Kafka_SUMMIT_DE238.json`
3. Update only the Kafka credentials in the source parameters
4. Enable controller services and start the flow

## Parameters to configure

The connector has three parameter contexts:

### Source Parameters

| Parameter | Description | Default |
|---|---|---|
| Kafka Bootstrap Servers | Comma-separated broker list | *(your Kafka cluster)* |
| Kafka SASL Username | SASL authentication username | `tms-user-1` |
| Kafka SASL Password | SASL authentication password (sensitive) | |
| Kafka Security Protocol | `SASL_SSL` or `SASL_PLAINTEXT` | `SASL_SSL` |
| Kafka SASL Mechanism | `SCRAM-SHA-256` / `SCRAM-SHA-512` / `PLAIN` | `SCRAM-SHA-256` |

### Ingestion Parameters

| Parameter | Description | Default |
|---|---|---|
| Kafka Topics | Topic name(s) or regex pattern | `tms-.*` |
| Kafka Topic Format | `names` (comma-separated) or `pattern` (regex) | `pattern` |
| Kafka Group Id | Consumer group identifier | `<KAFKA_USERNAME>-group` |
| Kafka Auto Offset Reset | `earliest` or `latest` | `latest` |

### Destination Parameters

| Parameter | Description | Default |
|---|---|---|
| Destination Database | Target Snowflake database | `SUMMIT_DB_DEV` |
| Destination Schema | Target schema | `RAW` |
| Snowflake Role | Role for the runtime | `SUMMIT_INGEST_ROLE_DEV` |
| Snowflake Authentication Strategy | `SNOWFLAKE_MANAGED` or `KEY_PAIR` | `SNOWFLAKE_MANAGED` |

When using `SNOWFLAKE_MANAGED` (recommended), the runtime authenticates using its assigned role — no username/key needed.

## Topic-to-table mapping

The **Map Topic to Table** processor transforms Kafka topic names into Snowflake table names using the expression:

```
${kafka.topic:substringAfter('tms-'):replace('-', '_'):toUpper()}
```

This maps:
- `tms-orders` → `ORDERS`
- `tms-order-items` → `ORDER_ITEMS`
- `tms-tracking-events` → `TRACKING_EVENTS`

## Features

- **Schema evolution** — new fields in JSON messages automatically add columns to the destination table
- **Dead letter queue** — malformed messages are routed to a DLQ topic rather than blocking ingestion
- **Multi-topic** — a single connector consumes from multiple topics using regex patterns (`tms-.*`)
- **Snowflake-managed auth** — no credentials to manage when running inside an Openflow deployment
- **Expression-based mapping** — topic-to-table name transformation without hardcoded mappings
