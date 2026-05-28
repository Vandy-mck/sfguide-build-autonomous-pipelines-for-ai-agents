# Fraud Detection — AI-Powered Payment Scoring

AI-powered fraud detection pipeline that scores payments using heuristic signals and enriches flagged transactions with Cortex AI (`AI_CLASSIFY` and `AI_COMPLETE`). Includes a standalone detector, a Jupyter notebook for batch scoring against Snowflake, and example test data.

## Overview

The fraud detection pipeline has two stages:

### Stage 1 — Heuristic Scoring

Evaluates 6 independent signals for each payment:

| Signal | Weight | Trigger |
|---|---|---|
| `billing_country_mismatch` | +0.35 | Billing country ≠ customer country |
| `card_country_mismatch` | +0.30 | Card issuing country ≠ customer country |
| `ip_geolocation_mismatch` | +0.25 | IP prefix doesn't match customer country |
| `known_fraud_device` | +0.40 | Device fingerprint in known-fraud pool |
| `velocity_abuse` | +0.30 | 5+ payments from same customer in 5 minutes |
| `high_declared_value` | +0.20 | Total declared value > EUR 20,000 |

A payment is flagged as fraudulent when its combined score reaches **0.30** or higher.

### Stage 2 — AI Enrichment (Cortex AI)

Flagged payments are enriched using Snowflake Cortex AI functions:

| Function | Purpose |
|---|---|
| `AI_CLASSIFY` | Classifies each flagged payment into a fraud type: `identity_theft`, `card_testing`, `account_takeover`, `synthetic_identity`, or `friendly_fraud` |
| `AI_COMPLETE` (mistral-large2) | Generates a 2–3 sentence natural language explanation of why the payment is fraudulent |

The classification uses a detailed prompt with the signal breakdown and classification rules to produce accurate fraud type labels.

## Files

| File | Description |
|---|---|
| `tms_fraud_detector.py` | Standalone fraud scoring heuristics (JSON input/output) |
| `fraud_detection_notebook.ipynb` | Jupyter notebook — scores payments, AI-enriches flagged ones, writes results to Snowflake |
| `example_fraud_payments.json` | Sample payment records with known fraud patterns for testing |

## Standalone Fraud Detector

Score payments from a JSON file:

```bash
python3 5_fraud_detection/tms_fraud_detector.py --input payments.json
python3 5_fraud_detection/tms_fraud_detector.py --input payments.json --threshold 0.5
python3 5_fraud_detection/tms_fraud_detector.py --input payments.json --output results.json
```

### Input format

The input must be a JSON array of records:

```json
[
  {
    "payment": { "payment_id": 1, "amount": 500, ... },
    "order": { "order_id": 1, ... },
    "items": [ ... ],
    "customer": { "id": 1, "country": "Germany", ... }
  }
]
```

## Fraud Detection Notebook

The notebook (`fraud_detection_notebook.ipynb`) runs the full pipeline against Snowflake:

1. Connects to Snowflake and reads payments, orders, items, and customers from `DT_CLEAN_*` tables
2. Runs each payment through the 6-signal heuristic fraud scorer
3. Writes scored results to `SUMMIT_DB_DEV.TRANSFORM.FRAUD_DETECTION_RESULTS`
4. **AI Enrichment** — uses `AI_CLASSIFY` to assign a fraud type and `AI_COMPLETE` to generate an explanation for each flagged payment
5. Updates the results table with `FRAUD_TYPE` and `EXPLANATION` columns

The `DT_FRAUD_DETECTION` dynamic table automatically enriches these results with payment details, customer info, and order context.

### Running the notebook

```bash
# Generate orders with higher fraud rate first
python3 3_generate/tms_producer.py --count 50 --delay 0.5 --fraud-rate 0.10

# Then run the notebook in Jupyter or Snowflake Notebooks
jupyter notebook 5_fraud_detection/fraud_detection_notebook.ipynb
```

### Output columns

| Column | Description |
|---|---|
| `PAYMENT_ID` | Payment identifier |
| `ORDER_ID` | Associated order |
| `CUSTOMER_ID` | Customer who made the payment |
| `FRAUD_SCORE` | Heuristic score (0.0–1.0) |
| `IS_FRAUD` | True if score ≥ 0.30 |
| `FRAUD_SIGNALS` | Comma-separated list of triggered signals |
| `PAYMENT_AMOUNT` | Payment value in EUR |
| `PAYMENT_TIMESTAMP` | When the payment was made |
| `SCORED_AT` | When the scoring was performed |
| `FRAUD_TYPE` | AI-classified fraud category (only for flagged payments) |
| `EXPLANATION` | AI-generated natural language explanation (only for flagged payments) |

### Verify results

```sql
SELECT COUNT(*) AS total_scored,
       SUM(CASE WHEN IS_FRAUD THEN 1 ELSE 0 END) AS flagged_fraud,
       ROUND(AVG(FRAUD_SCORE), 3) AS avg_score
FROM SUMMIT_DB_DEV.TRANSFORM.FRAUD_DETECTION_RESULTS;

SELECT PAYMENT_ID, FRAUD_SCORE, FRAUD_TYPE, EXPLANATION
FROM SUMMIT_DB_DEV.TRANSFORM.FRAUD_DETECTION_RESULTS
WHERE IS_FRAUD = TRUE
ORDER BY FRAUD_SCORE DESC LIMIT 5;
```

## Integration with Dynamic Tables

The scored results feed into the `DT_FRAUD_DETECTION` dynamic table (1-minute refresh) which joins fraud results with payment, order, and customer details. The `ANALYTICS.FRAUD_DETECTION` view exposes this for dashboards and the Cortex Agent.

```sql
SELECT * FROM SUMMIT_DB_DEV.ANALYTICS.FRAUD_DETECTION
WHERE IS_FRAUD = TRUE
ORDER BY FRAUD_SCORE DESC LIMIT 10;
```
