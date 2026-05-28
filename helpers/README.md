# Helpers — Reference and Utility Scripts

Utility scripts for environment setup, connectivity testing, user management, and cleanup.

## Files

| File | Description |
|---|---|
| `setup_snow_cli_connection.sh` | Configure a named Snowflake CLI connection using `.env` credentials (PAT auth) |
| `setup_rpk_profile.sh` | Create a local `rpk` profile for Kafka/Redpanda using `.env` credentials |
| `test_kafka_connection.py` | Verify connectivity to the Kafka cluster |
| `setup_git_repo.sql` | Configure a Git repository integration in Snowflake (for CI/CD) |
| `cleanup_tables.sql` | Drop all dynamic tables, views, and truncate RAW tables (for a clean re-deploy) |

## Usage

All scripts expect a `.env` file in the project root. See `.env.template` for required variables.

### Set up Snowflake CLI connection

```bash
source .env
bash helpers/setup_snow_cli_connection.sh
```

Creates a named connection using a Programmatic Access Token (PAT). Verifies connectivity on completion.

### Set up Redpanda CLI profile

```bash
source .env
bash helpers/setup_rpk_profile.sh              # uses KAFKA_USERNAME as profile name
bash helpers/setup_rpk_profile.sh my-profile   # custom profile name
```

Configures SASL/SCRAM-SHA-256 over TLS for the Kafka cluster.

### Test Kafka connectivity

```bash
source .env
python3 helpers/test_kafka_connection.py
```

Connects to the Kafka cluster, lists topics, and reports success/failure.

### Configure Git repository (for CI/CD)

Run `setup_git_repo.sql` in Snowsight or via CLI after replacing placeholders:

- `<USERNAME>` — your Snowflake username
- `<GITHUB_USERNAME>` — your GitHub username
- `<GITHUB_PAT>` — a GitHub Personal Access Token

### Cleanup tables (reset data)

Run `cleanup_tables.sql` to drop all dynamic tables and views, then truncate all RAW tables. Useful before re-deploying the DCM project or re-seeding data.

```bash
snow sql -f helpers/cleanup_tables.sql
```
