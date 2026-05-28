"""Fraud detection heuristics for TMS payment scoring.

Usage:
    python3 fraud_detector.py --input payments.json
    python3 fraud_detector.py --input payments.json --threshold 0.5
    python3 fraud_detector.py --input payments.json --output results.json

Input JSON format (file must contain a JSON array of records):
    [
      {
        "payment": { ... },
        "order": { ... },
        "items": [ ... ],
        "customer": { "id": 1, "country": "Germany", ... }
      },
      ...
    ]

Arguments:
    --input       Path to JSON file with payment records (required)
    --output      Path to write results JSON (default: stdout)
    --threshold   Fraud score threshold to flag as fraud (default: 0.30)
"""

import argparse
import json
import sys
from datetime import datetime, timedelta

COUNTRY_IP_PREFIXES = {
    "Germany": "91.23", "France": "82.45", "Netherlands": "145.90", "Czech Republic": "78.128",
    "Switzerland": "178.82", "Poland": "185.45", "Austria": "77.116", "Italy": "151.38",
    "Denmark": "212.88", "Spain": "88.12", "Belgium": "109.23", "Sweden": "83.55",
    "Ireland": "86.45", "Romania": "79.114", "Finland": "91.152", "Portugal": "95.92",
    "Hungary": "84.206", "Bulgaria": "78.83", "Greece": "94.66", "United Kingdom": "81.174",
}

FRAUD_DEVICE_POOL = ["fp_fraud_000", "fp_fraud_001", "fp_fraud_002", "fp_fraud_003", "fp_fraud_004"]

_recent_payments = []


def detect_fraud(payment, order, items, customer, threshold=0.30):
    """Score a payment for fraud risk based on multiple heuristic signals.

    Checks six independent fraud indicators and sums their weighted
    scores. A payment is flagged as fraudulent when the combined score
    reaches the threshold or higher.

    Signals and weights:
        billing_country_mismatch  (+0.35) — billing country ≠ customer country.
        card_country_mismatch     (+0.30) — card issuing country ≠ customer country.
        ip_geolocation_mismatch   (+0.25) — IP prefix doesn't match customer country.
        known_fraud_device        (+0.40) — device fingerprint in FRAUD_DEVICE_POOL.
        velocity_abuse            (+0.30) — 5+ payments from same customer in 5 min.
        high_declared_value       (+0.20) — total declared value > EUR 20,000.

    Maintains a sliding window of recent payments (max 500) for velocity
    detection across consecutive calls.

    Args:
        payment: Payment dict as produced by generate_order().
        order: Order dict as produced by generate_order().
        items: List of order item dicts for this order.
        customer: Customer dict from the CUSTOMERS list.
        threshold: Score at or above which a payment is flagged (default 0.30).

    Returns:
        Dict with keys:
            is_fraud (bool): True if score >= threshold.
            fraud_score (float): Combined score capped at 1.0.
            signals (list[str]): Names of triggered indicators.
    """
    signals = []
    score = 0.0

    if payment["billing_country"] and payment["billing_country"] != customer["country"]:
        signals.append("billing_country_mismatch")
        score += 0.35

    if payment["card_country"] and payment["card_country"] != customer["country"]:
        signals.append("card_country_mismatch")
        score += 0.30

    ip = payment.get("ip_address", "")
    ip_prefix = ".".join(ip.split(".")[:2]) if ip else ""
    expected_prefix = COUNTRY_IP_PREFIXES.get(customer["country"], "")
    if ip_prefix and expected_prefix and ip_prefix != expected_prefix:
        signals.append("ip_geolocation_mismatch")
        score += 0.25

    fp = payment.get("device_fingerprint", "")
    if fp in FRAUD_DEVICE_POOL:
        signals.append("known_fraud_device")
        score += 0.40

    cutoff = datetime.fromisoformat(payment["payment_timestamp"]) - timedelta(minutes=5)
    recent_same_customer = [
        p for p in _recent_payments
        if p["customer_id"] == customer["id"]
        and datetime.fromisoformat(p["timestamp"]) > cutoff
    ]
    if len(recent_same_customer) >= 5:
        signals.append("velocity_abuse")
        score += 0.30

    total_declared = sum(i.get("declared_value", 0) for i in items)
    if total_declared > 20000:
        signals.append("high_declared_value")
        score += 0.20

    _recent_payments.append({
        "customer_id": customer["id"],
        "timestamp": payment["payment_timestamp"],
    })
    if len(_recent_payments) > 500:
        _recent_payments.pop(0)

    return {
        "is_fraud": score >= threshold,
        "fraud_score": round(min(score, 1.0), 2),
        "signals": signals,
    }


def main():
    parser = argparse.ArgumentParser(description="TMS Fraud Detector — score payments from a JSON file")
    parser.add_argument("--input", required=True, help="Path to JSON file with payment records")
    parser.add_argument("--output", default=None, help="Path to write results JSON (default: stdout)")
    parser.add_argument("--threshold", type=float, default=0.30, help="Fraud score threshold (default: 0.30)")
    args = parser.parse_args()

    with open(args.input, "r") as f:
        records = json.load(f)

    if isinstance(records, dict):
        records = [records]

    results = []
    flagged = 0
    for rec in records:
        detection = detect_fraud(
            payment=rec["payment"],
            order=rec["order"],
            items=rec["items"],
            customer=rec["customer"],
            threshold=args.threshold,
        )
        result = {
            "order_id": rec["order"].get("order_id"),
            "payment_id": rec["payment"].get("payment_id"),
            **detection,
        }
        results.append(result)
        if detection["is_fraud"]:
            flagged += 1

    output_json = json.dumps(results, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output_json)
        print(f"Scored {len(results)} payments — {flagged} flagged as fraud. Results written to {args.output}")
    else:
        print(output_json)
        print(f"\nScored {len(results)} payments — {flagged} flagged as fraud.", file=sys.stderr)


if __name__ == "__main__":
    main()
