# Bootstrap — Account Preparation

Run `setup.sql` as **ACCOUNTADMIN** before creating the DCM project. The script is idempotent — safe to run multiple times.

## Usage

```bash
snow sql -f 1_bootstrap/setup.sql -c <connection_name>
```

The connection must use the `ACCOUNTADMIN` role. See `helpers/setup_snow_cli_connection.sh` to configure one.

## What the script does

### 1. Create the SUMMIT_ADMIN role

Creates a role named `SUMMIT_ADMIN` and grants it to the current user. The role receives the following account-level privileges:

| Privilege | Purpose |
|---|---|
| CREATE DATABASE | Create project databases (`DCM_DB`, `SUMMIT_DB_DEV`) |
| CREATE WAREHOUSE | Create compute warehouses (`SUMMIT_WH`, `SUMMIT_WH_DEV`) |
| CREATE ROLE | Create additional roles (e.g. `SUMMIT_DEVELOPER_ROLE_DEV`, `SUMMIT_INGEST_ROLE_DEV`) |
| CREATE EXTERNAL ACCESS INTEGRATION | Allow SPCS/Openflow runtimes to reach external endpoints (Kafka) |
| CREATE INTEGRATION | Create notification and other integrations |
| EXECUTE MANAGED TASK / EXECUTE TASK | Schedule and run tasks |
| MANAGE GRANTS | Grant privileges to other roles |
| CREATE OPENFLOW DEPLOYMENT | Provision Openflow deployments |
| BIND SERVICE ENDPOINT (with grant option) | Expose SPCS service endpoints |
| CREATE COMPUTE POOL (with grant option) | Create compute pools for SPCS |
| SNOWFLAKE.EVENTS_VIEWER application role | View telemetry and event data |
| SNOWFLAKE.CORTEX_USER database role | Use Cortex AI functions |

### 2. Create DCM project resources

Switches to `SUMMIT_ADMIN` and creates:

- **Database & schema** — `DCM_DB.PROJECTS`
- **Warehouse** — `SUMMIT_WH` (X-SMALL, auto-suspend 300 s)
- **DCM project** — `DCM_DB.PROJECTS.DCM_PROJECT_DEV`

### 3. Provision the Openflow deployment

Creates an Openflow deployment named `SUMMIT_DEPLOYMENT` (type `SNOWFLAKE`). This takes **8–10 minutes** to provision — run it early.

### 4. Configure network access for Kafka/Redpanda

Creates a network rule and external access integration so that Openflow SPCS runtimes can reach the Kafka cluster:

- **Network rule** — `DCM_DB.NETWORK.REDPANDA_NETWORK_RULE` (egress to the Redpanda broker endpoints on port 9092)
- **External access integration** — `SUMMIT_EAI` (references the network rule above)

### 5. Retrieve account identifier

Outputs `CURRENT_ORGANIZATION_NAME()-CURRENT_ACCOUNT_NAME()` and `CURRENT_USER()` — use these values to update `2_dcm_project/manifest.yml`.

### 6. Tear down (optional)

The last section of the script (commented out) terminates the Openflow deployment and drops the database, warehouse, and role:

```sql
USE ROLE ACCOUNTADMIN;
ALTER OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT TERMINATE;
DROP DATABASE IF EXISTS DCM_DB;
DROP WAREHOUSE IF EXISTS SUMMIT_WH;
DROP ROLE IF EXISTS SUMMIT_ADMIN;
DROP INTEGRATION SUMMIT_EAI;
```
