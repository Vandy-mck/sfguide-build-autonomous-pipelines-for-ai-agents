# Data Generator — TMS Producer & Consumer

Synthetic data generator for the Transportation Management System (TMS). The producer publishes order lifecycle events to Kafka/Redpanda; the consumer reads and displays them.

## Prerequisites

### Python environment

```bash
source .venv/bin/activate
```

### Environment file

All scripts read parameters from environment variables (`.env`). Copy the template and fill in your values:

```bash
source .env
```

### Redpanda CLI (`rpk`)

Install the `rpk` CLI to manage topics and verify connectivity:

```bash
brew install redpanda-data/tap/redpanda
```

### Configure `rpk` profile

Create a profile using your `.env` credentials so `rpk` commands authenticate automatically:

```bash
source .env
bash helpers/setup_rpk_profile.sh
```

### Verify connectivity

```bash
rpk cluster info
rpk topic list
```

### Create topics

If topics don't exist yet, create them using your configured prefix:

```bash
source .env
for suffix in orders order-items payments packages tracking-events deliveries; do
  rpk topic create "${KAFKA_TOPIC_PREFIX}-${suffix}"
done
rpk topic list
```

### Delete topics (cleanup)

```bash
source .env
for suffix in orders order-items payments packages tracking-events deliveries; do
  rpk topic delete "${KAFKA_TOPIC_PREFIX}-${suffix}"
done
```

## Topics

Each new generated order produces ~17 messages across 6 topics:

| Topic | Content |
|---|---|
| `tms-orders` | One message per order |
| `tms-order-items` | One message per line item (1–3 per order) |
| `tms-payments` | One message per payment (with fraud score) |
| `tms-packages` | One message per package |
| `tms-tracking-events` | Multiple messages per package (multi-hop journey) |
| `tms-deliveries` | One message per completed/failed delivery |

## Producer

### Basic usage

```bash
source .env
python3 3_generate/tms_producer.py
```

### Dry run (print to stdout, no Kafka)

```bash
python3 3_generate/tms_producer.py --dry-run --count 5 --delay 0
```

### Generate with higher fraud rate

```bash
python3 3_generate/tms_producer.py --count 50 --delay 0.5 --fraud-rate 0.10
```

### Backfill historical data

```bash
python3 3_generate/tms_producer.py --days-back 30 --count 5
```

### Reset state and start fresh

```bash
python3 3_generate/tms_producer.py --reset-state --count 5
```

### State management

ID counters are persisted to `.tms_producer_state.json` after each order. On restart the producer resumes from the last saved state. Use `--reset-state` to delete the file and start from seed IDs.

## Consumer

### Basic usage

```bash
source .env
python3 3_generate/tms_consumer.py
```

### Read from beginning

```bash
python3 3_generate/tms_consumer.py --from-beginning
```

### Additional consumer flags

| Flag | Default | Description |
|---|---|---|
| `--group-id` | `tms-consumer-group` | Kafka consumer group |
| `--from-beginning` | off | Start from earliest offset |
| `--timeout` | `30000` | Consumer poll timeout in ms |

## Files

| File | Description |
|---|---|
| `tms_producer.py` | Synthetic order data producer (Kafka) |
| `tms_consumer.py` | Topic consumer with formatted output |
| `tms_ddl.sql` | Table DDL and schema diagram (reference only — tables are managed by the DCM project) |
| `tms_seed_data.sql` | Standalone seed data (reference only — use `2_dcm_project/scripts/seed_data.sql` instead) |
