# DCM Project — TMS Data Platform

This folder contains the DCM (Database Change Management) project that defines the entire TMS (Transportation Management System) data platform as code.

## Project structure

```
2_dcm_project/
├── manifest.yml                        # Targets (DEV / STAGE / PROD) and templating config
├── sources/definitions/
│   ├── database.sql                    # Database, schemas, and warehouse
│   ├── roles.sql                       # SUMMIT_DEVELOPER_ROLE, SUMMIT_INGEST_ROLE
│   ├── raw_tables.sql                  # 9 raw tables (change-tracking enabled)
│   ├── transform.sql                   # 14 dynamic tables (2 layers) + FRAUD_DETECTION_RESULTS table
│   └── analytics.sql                   # 5 analytics views
└── scripts/
    ├── seed_data.sql                   # Reference data + 1 sample end-to-end order
    ├── post_deploy.sql                 # Openflow runtime creation and grants
    └── tear_down.sql                   # Drop all resources
```

## What gets created

### Infrastructure (`database.sql`)

- **Database** — `SUMMIT_DB{env_suffix}`
- **Schemas** — `OPENFLOW`, `RAW`, `TRANSFORM`, `ANALYTICS`
- **Warehouse** — `SUMMIT_WH{env_suffix}` (X-SMALL, auto-suspend 300 s)

### Roles (`roles.sql`)

| Role | Purpose |
|---|---|
| `SUMMIT_DEVELOPER_ROLE{env_suffix}` | Full access — Openflow, dynamic tables, semantic views, agents. SELECT on all DTs and analytics views. INSERT/UPDATE/TRUNCATE on TRANSFORM tables. |
| `SUMMIT_INGEST_ROLE{env_suffix}` | Write access to `RAW` schema, CREATE TABLE, used by Openflow runtime. Granted to SUMMIT_DEVELOPER_ROLE. |

Both roles are granted up to `SUMMIT_ADMIN` (the project owner).

### Raw tables (`raw_tables.sql`)

Nine tables in the `RAW` schema, all with change tracking enabled:

`CUSTOMERS`, `SHIPPING_PRODUCTS`, `LOCATIONS`, `ORDERS`, `ORDER_ITEMS`, `PAYMENTS`, `PACKAGES`, `TRACKING_EVENTS`, `DELIVERIES`

### Dynamic tables — Layer 1: Clean (`transform.sql`)

Nine dynamic tables in the `TRANSFORM` schema (1-minute target lag). Each deduplicates on the primary key, TRIMs strings, and standardizes categorical values with UPPER:

| Dynamic Table | Source RAW table |
|---|---|
| `DT_CLEAN_CUSTOMERS` | CUSTOMERS |
| `DT_CLEAN_SHIPPING_PRODUCTS` | SHIPPING_PRODUCTS |
| `DT_CLEAN_LOCATIONS` | LOCATIONS |
| `DT_CLEAN_ORDERS` | ORDERS |
| `DT_CLEAN_ORDER_ITEMS` | ORDER_ITEMS |
| `DT_CLEAN_PAYMENTS` | PAYMENTS |
| `DT_CLEAN_PACKAGES` | PACKAGES |
| `DT_CLEAN_TRACKING_EVENTS` | TRACKING_EVENTS |
| `DT_CLEAN_DELIVERIES` | DELIVERIES |

### Dynamic tables — Layer 2: Analytics (`transform.sql`)

Five analytic dynamic tables that join across the clean layer to build business-ready aggregations:

| Dynamic Table | Upstream Clean DTs | Description |
|---|---|---|
| `DT_ORDER_SUMMARY` | CUSTOMERS, ORDERS, ORDER_ITEMS, PACKAGES, DELIVERIES | Denormalized order view with customer, items, packages, delivery |
| `DT_PACKAGE_TRACKING` | PACKAGES, ORDERS, CUSTOMERS, TRACKING_EVENTS | Package-level tracking with hub count and transit hours |
| `DT_PACKAGE_HOPS` | TRACKING_EVENTS, LOCATIONS, PACKAGES | Hop-by-hop journey for each package |
| `DT_LOCATION_ACTIVITY` | LOCATIONS, TRACKING_EVENTS, PACKAGES | Daily throughput and dwell-time percentiles per location |
| `DT_FRAUD_DETECTION` | FRAUD_DETECTION_RESULTS, PAYMENTS, ORDERS, CUSTOMERS | Payment fraud scoring with enriched context |

### Standalone table (`transform.sql`)

| Table | Description |
|---|---|
| `FRAUD_DETECTION_RESULTS` | Written by the fraud detection notebook. Contains scored payments with fraud signals. |

### Analytics views (`analytics.sql`)

Five views in the `ANALYTICS` schema — thin pass-through views on top of the Layer 2 dynamic tables:

| View | Source DT |
|---|---|
| `ORDER_SUMMARY` | DT_ORDER_SUMMARY |
| `PACKAGE_TRACKING` | DT_PACKAGE_TRACKING |
| `PACKAGE_HOPS` | DT_PACKAGE_HOPS |
| `LOCATION_ACTIVITY` | DT_LOCATION_ACTIVITY |
| `FRAUD_DETECTION` | DT_FRAUD_DETECTION |

## Pipeline architecture

```
  RAW                          TRANSFORM Layer 1         TRANSFORM Layer 2        ANALYTICS
  ─────────────────            ─────────────────         ─────────────────        ─────────
  CUSTOMERS ────────────────► DT_CLEAN_CUSTOMERS ─────┐
  SHIPPING_PRODUCTS ────────► DT_CLEAN_SHIPPING_PRODS │
  LOCATIONS ────────────────► DT_CLEAN_LOCATIONS ─────┤
  ORDERS ───────────────────► DT_CLEAN_ORDERS ────────┤
  ORDER_ITEMS ──────────────► DT_CLEAN_ORDER_ITEMS ───┼──► DT_ORDER_SUMMARY ────► ORDER_SUMMARY
  PAYMENTS ─────────────────► DT_CLEAN_PAYMENTS ──────┼──► DT_PACKAGE_TRACKING ─► PACKAGE_TRACKING
  PACKAGES ─────────────────► DT_CLEAN_PACKAGES ──────┼──► DT_PACKAGE_HOPS ─────► PACKAGE_HOPS
  TRACKING_EVENTS ──────────► DT_CLEAN_TRACKING_EVENTS┼──► DT_LOCATION_ACTIVITY ► LOCATION_ACTIVITY
  DELIVERIES ───────────────► DT_CLEAN_DELIVERIES ────┘
                                                          DT_FRAUD_DETECTION ───► FRAUD_DETECTION
                                                               ▲
                              FRAUD_DETECTION_RESULTS ──────────┘
                              (populated by notebook)
```

## Data model

```
┌─────────────────┐       ┌──────────────────────┐
│   CUSTOMERS     │       │  SHIPPING_PRODUCTS   │
│─────────────────│       │──────────────────────│
│ customer_id PK  │       │ product_id PK        │
│ customer_type   │       │ name                 │
│ first_name      │       │ service_type         │
│ last_name       │       │ zone                 │
│ company_name    │       │ base_price           │
│ email           │       │ max_weight/dims      │
│ city, country   │       │ estimated_days       │
└────────┬────────┘       └──────────┬───────────┘
         │ 1:N                       │ 1:N
         ▼                           ▼
┌──────────────────────────────────────────────┐
│                  ORDERS                      │
│──────────────────────────────────────────────│
│ order_id PK                                  │
│ customer_id FK ──► CUSTOMERS                 │
│ origin_location_id FK ──► LOCATIONS          │
│ pickup_address/city/country                  │
│ destination_address/city/country             │
│ total_amount, currency                       │
└───────┬──────────────────────────┬───────────┘
        │ 1:N                      │ 1:1
        ▼                          ▼
┌─────────────────────────┐       ┌─────────────────────────┐
│  ORDER_ITEMS            │       │       PAYMENTS          │
│─────────────────────────│       │─────────────────────────│
│ order_item_id PK        │       │ payment_id PK           │
│ order_id FK ─► ORDERS   │       │ order_id FK ──► ORDERS  │
│ product_id FK ─►        │       │ payment_method          │
│   SHIPPING_PRODUCTS     │       │ card_last_four/brand    │
│ quantity                │       │ card_country            │
│ declared_contents       │       │ billing_addr/city/ctry  │
│ declared_value          │       │ ip_address              │
│ insurance_opted         │       │ device_fingerprint      │
└────────┬────────────────┘       └─────────────────────────┘
         │ 1:1                        ▲ fraud detection
         ▼
┌─────────────────────────┐     ┌────────────────────┐
│     PACKAGES            │     │   LOCATIONS        │
│─────────────────────────│     │────────────────────│
│ package_id PK           │     │ location_id PK     │
│ order_item_id FK ──►    │     │ name               │
│   ORDER_ITEMS           │     │ type (HUB/WH/      │
│ order_id FK ──► ORDERS  │     │   OFFICE/PICKUP)   │
│ tracking_number         │     │ city, country      │
│ actual_weight_kg        │     │ lat, lng           │
│ status                  │     │ capacity           │
└───────┬─────────────────┘     └─────┬──────────────┘
        │ 1:N                         │ 1:N
        ▼                             │
┌──────────────────────┐              │
│  TRACKING_EVENTS     │              │
│──────────────────────│              │
│ event_id PK          │              │
│ package_id FK ─► PKG │              │
│ location_id FK ──────┼──────────────┘
│ event_timestamp      │
│ event_type           │
│ carrier              │
└───────┬──────────────┘
        │ 1:1
        ▼
┌──────────────────────┐
│    DELIVERIES        │
│──────────────────────│
│ delivery_id PK       │
│ package_id FK ─► PKG │
│ driver_name          │
│ actual_delivery_date │
│ signature_collected  │
│ status               │
└──────────────────────┘

Flow: CUSTOMER ─► ORDER ─► ORDER_ITEMS ─► PACKAGES ─► TRACKING_EVENTS ─► DELIVERIES
                    │                                        │
                    └──► PAYMENT                             └──► LOCATIONS
```

## Usage

### Plan and deploy

```bash
snow dcm plan  --target DCM_DEV --from 2_dcm_project
snow dcm deploy --target DCM_DEV --from 2_dcm_project
```

### Post-deploy (Openflow runtime + grants)

```bash
snow sql -f 2_dcm_project/scripts/post_deploy.sql \
  --variable "env_suffix=_DEV" --enable-templating JINJA
```

### Seed reference data

```bash
snow sql -f 2_dcm_project/scripts/seed_data.sql \
  --variable "env_suffix=_DEV" --enable-templating JINJA
```

## Templating

The manifest uses Jinja templating with an `env_suffix` variable (`_DEV`, `_STAGE`, `_PROD`) so the same definitions can target multiple environments. Update `manifest.yml` with your account identifier before deploying.

## Tear down

To remove all resources created by this project:

```bash
snow sql -f 2_dcm_project/scripts/tear_down.sql \
  --variable "env_suffix=_DEV" --enable-templating JINJA
```

This drops resources in dependency order:

1. Suspend and drop the Openflow runtime
2. Drop the database (`SUMMIT_DB_DEV`) — removes all schemas, tables, dynamic tables, views, and data
3. Drop the warehouse (`SUMMIT_WH_DEV`)
4. Drop the roles (`SUMMIT_INGEST_ROLE_DEV`, `SUMMIT_DEVELOPER_ROLE_DEV`)
5. Drop the DCM project object

Replace `_DEV` with `_STAGE` or `_PROD` for other environments.
