"""TMS Consumer — read and display TMS order data from Kafka/Redpanda topics.

Subscribes to the 6 TMS topics produced by tms_producer.py and prints
each message to stdout with topic-specific formatting.

Usage:
    # Consume from all TMS topics (default settings)
    python3 tms_consumer.py

    # Custom prefix / cluster
    python3 tms_consumer.py --topic-prefix prod-tms --bootstrap-servers host:9092

    # Consume from beginning
    python3 tms_consumer.py --from-beginning

Requirements:
    pip install kafka-python snowflake snowpipe-streaming
"""

import argparse
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kafka import KafkaConsumer


TOPIC_ICONS = {
    "orders":          "O",
    "order-items":     "I",
    "payments":        "$",
    "packages":        "P",
    "tracking-events": "T",
    "deliveries":      "D",
}


def format_message(topic_suffix, key, value):
    """Format a consumed message for display based on its topic type.

    Args:
        topic_suffix: The topic name suffix (e.g. 'orders', 'tracking-events').
        key: Message key (order_id).
        value: Deserialized JSON message dict.

    Returns:
        Formatted one-line string for console output.
    """
    icon = TOPIC_ICONS.get(topic_suffix, "?")

    if topic_suffix == "orders":
        return (f"[{icon}] order={value.get('order_id')} customer={value.get('customer_id')} "
                f"status={value.get('status')} {value.get('pickup_city')}->{value.get('destination_city')} "
                f"amount={value.get('total_amount')} {value.get('currency')}")

    elif topic_suffix == "order-items":
        return (f"[{icon}] order={value.get('order_id')} item={value.get('order_item_id')} "
                f"product={value.get('product_id')} qty={value.get('quantity')} "
                f"contents=\"{value.get('declared_contents', '')[:40]}\" value={value.get('declared_value')}")

    elif topic_suffix == "payments":
        return (f"[{icon}] order={value.get('order_id')} method={value.get('payment_method')} "
                f"card={value.get('card_brand') or '-'}/{value.get('card_last_four') or '-'} "
                f"amount={value.get('payment_amount')} billing={value.get('billing_country')} "
                f"ip={value.get('ip_address')}")

    elif topic_suffix == "packages":
        return (f"[{icon}] order={value.get('order_id')} pkg={value.get('package_id')} "
                f"tracking={value.get('tracking_number')} type={value.get('package_type')} "
                f"weight={value.get('actual_weight_kg')}kg status={value.get('status')}")

    elif topic_suffix == "tracking-events":
        loc = value.get("location_id") or "last-mile"
        return (f"[{icon}] pkg={value.get('package_id')} {value.get('event_type'):<20s} "
                f"loc={loc} status={value.get('status')} "
                f"carrier={value.get('carrier')} | {value.get('description', '')[:50]}")

    elif topic_suffix == "deliveries":
        return (f"[{icon}] pkg={value.get('package_id')} status={value.get('status')} "
                f"driver={value.get('driver_name') or '-'} recipient={value.get('recipient_name')} "
                f"signed={value.get('signature_collected')} | {value.get('delivery_notes', '')[:40]}")

    return f"[?] {json.dumps(value)[:120]}"


def main():
    """CLI entry point — parse args, subscribe to topics, and print messages."""
    parser = argparse.ArgumentParser(description="TMS Consumer — read TMS data from Kafka/Redpanda")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--username", default=os.getenv("KAFKA_USERNAME", "kafka_user"))
    parser.add_argument("--password", default=os.getenv("KAFKA_PASSWORD", "kafka_pass"))
    parser.add_argument("--topic-prefix", default=os.getenv("KAFKA_TOPIC_PREFIX", "tms"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_USERNAME", "kafka_user_consumer_group") + "-group")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offset")
    parser.add_argument("--timeout", type=int, default=30000, help="Consumer timeout in ms (default: 30000)")
    parser.add_argument("--commit", action="store_true", help="Commit offsets (disable dry-run mode)")
    args = parser.parse_args()

    dry_run = not args.commit

    suffixes = ["orders", "order-items", "payments", "packages", "tracking-events", "deliveries"]
    topics = [f"{args.topic_prefix}-{s}" for s in suffixes]
    suffix_map = {f"{args.topic_prefix}-{s}": s for s in suffixes}

    consumer = KafkaConsumer(
        *topics,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=args.username,
        sasl_plain_password=args.password,
        api_version=(2, 8, 0),
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=not dry_run,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=args.timeout,
    )

    print(f"TMS Consumer | server={args.bootstrap_servers}")
    print(f"Topics: {', '.join(topics)}")
    print(f"Group: {args.group_id} | offset: {'earliest' if args.from_beginning else 'latest'} | mode: {'DRY-RUN' if dry_run else 'COMMIT'}")
    print(f"  [O] {topics[0]:<30s}  [I] {topics[1]}")
    print(f"  [$] {topics[2]:<30s}  [P] {topics[3]}")
    print(f"  [T] {topics[4]:<30s}  [D] {topics[5]}")
    print("=" * 100)

    counts = {s: 0 for s in suffixes}
    total = 0

    try:
        for message in consumer:
            topic_suffix = suffix_map.get(message.topic, message.topic)
            counts[topic_suffix] = counts.get(topic_suffix, 0) + 1
            total += 1

            line = format_message(topic_suffix, message.key, message.value)
            print(f"{total:>6}  {line}")
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        print("=" * 100)
        print(f"Total: {total} messages consumed")
        for s in suffixes:
            print(f"  [{TOPIC_ICONS[s]}] {s}: {counts[s]}")
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
