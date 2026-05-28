"""Test Kafka/Redpanda connectivity using credentials from .env.

Usage:
    python helpers/test_kafka_connection.py

Prerequisites:
    1. Copy .env.template to .env and fill in KAFKA_BOOTSTRAP_SERVERS,
       KAFKA_USERNAME, and KAFKA_PASSWORD.
    2. Install dependencies: pip install kafka-python python-dotenv
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kafka import KafkaConsumer
from kafka.errors import KafkaError

broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
username = os.getenv("KAFKA_USERNAME", "kafka_user")
password = os.getenv("KAFKA_PASSWORD", "kafka_pass")

print(f"Testing connection to: {broker}")
print(f"Username: {username}")

try:
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=username,
        sasl_plain_password=password,
        api_version=(2, 8, 0),
        request_timeout_ms=15000,
    )
    topics = consumer.topics()
    print(f"OK — connected. Visible topics ({len(topics)}): {', '.join(sorted(topics))}")
    consumer.close()
except KafkaError as e:
    print(f"FAILED: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
    sys.exit(1)
