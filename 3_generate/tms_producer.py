"""TMS Order Producer — continuous synthetic data generator for Kafka/Redpanda.

Generates realistic Transportation Management System order data and publishes
JSON messages across 6 Kafka topics. Each order produces ~17 messages covering
the full lifecycle: order, items, payment, packages, tracking events, delivery.

Topics:
    tms-orders           One message per order
    tms-order-items      One message per line item (1-3 per order)
    tms-payments         One message per payment (enriched with fraud score)
    tms-packages         One message per package (1 per order item)
    tms-tracking-events  Multiple messages per package (multi-hop journey)
    tms-deliveries       One message per completed/failed delivery

Reference data (embedded, not fetched from Snowflake):
    50 European customers (companies and individuals)
    20 logistics locations (hubs, warehouses, offices, pickup points)
     8 shipping products (parcels, pallets, envelopes — standard/express)

Features:
    - Multi-hop routing based on haversine distance (2-4 hub stops)
    - Configurable fraud injection with 4 patterns + real-time detection
    - Configurable package return rate (damage/delivery failure)
    - 2% transit delay injection (3-7 day exceptions)
    - Payment fraud scoring with 6 heuristic signals
    - Persistent ID state file to prevent duplicate IDs across restarts

State management:
    ID counters are saved to .tms_producer_state.json after each order.
    On restart the producer loads the saved state and continues from where
    it left off. Use --reset-state to delete the file and start fresh.

Usage:
    # Dry run — print JSON to stdout
    python3 tms_producer.py --dry-run --count 5 --delay 0

    # Send to Kafka at 1 order/sec (default)
    python3 tms_producer.py --count 5

    # Resume after a previous run (automatic — reads .tms_producer_state.json)
    python3 tms_producer.py --count 5

    # Reset state and start from seed IDs
    python3 tms_producer.py --reset-state --count 5

    # Custom rates
    python3 tms_producer.py --fraud-rate 0.05 --return-rate 0.10 --count 5

    # Spread orders over the past 30 days
    python3 tms_producer.py --days-back 30 --count 5

    # Current timestamps only (real-time mode)
    python3 tms_producer.py --days-back 0 --count 5

    # Custom Kafka cluster
    python3 tms_producer.py --bootstrap-servers host:9092 --username user --password pass

    # Custom topic prefix
    python3 tms_producer.py --topic-prefix prod-tms

Arguments:
    --bootstrap-servers  Kafka broker address (default: localhost:9092)
    --username           SASL username (default: kafka_user)
    --password           SASL password (default: kafka_pass)
    --topic-prefix       Prefix for all topic names (default: tms)
    --count              Number of orders to generate (default: 1)
    --delay              Seconds between orders (default: 1.0)
    --fraud-rate         Fraction of orders with injected fraud (default: 0.01)
    --return-rate        Fraction of packages returned (default: 0.02)
    --days-back          Spread orders over the past N days (default: 7)
    --dry-run            Print JSON instead of sending to Kafka
    --reset-state        Delete saved state file and start from seed IDs

Requirements:
    pip install kafka-python snowflake snowpipe-streaming
"""

import argparse
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


CUSTOMERS = [
    {"id": 1,  "type": "COMPANY",    "name": "Schneider Electronics GmbH",   "city": "Berlin",      "country": "Germany",        "postal": "10178"},
    {"id": 2,  "type": "INDIVIDUAL", "name": "Marie Dupont",                 "city": "Paris",       "country": "France",         "postal": "75008"},
    {"id": 3,  "type": "COMPANY",    "name": "Van der Berg Imports BV",      "city": "Amsterdam",   "country": "Netherlands",    "postal": "1015"},
    {"id": 4,  "type": "COMPANY",    "name": "TechStart Solutions s.r.o.",    "city": "Prague",      "country": "Czech Republic", "postal": "11000"},
    {"id": 5,  "type": "INDIVIDUAL", "name": "Hans Meier",                   "city": "Zurich",      "country": "Switzerland",    "postal": "8002"},
    {"id": 6,  "type": "COMPANY",    "name": "Kowalski Transport Sp. z o.o.","city": "Warsaw",      "country": "Poland",         "postal": "00-001"},
    {"id": 7,  "type": "INDIVIDUAL", "name": "Wolfgang Fischer",             "city": "Vienna",      "country": "Austria",        "postal": "1060"},
    {"id": 8,  "type": "COMPANY",    "name": "Medici Pharma S.r.l.",         "city": "Milan",       "country": "Italy",          "postal": "20121"},
    {"id": 9,  "type": "INDIVIDUAL", "name": "Lars Pedersen",                "city": "Copenhagen",  "country": "Denmark",        "postal": "1620"},
    {"id": 10, "type": "COMPANY",    "name": "García Electrónica SL",        "city": "Madrid",      "country": "Spain",          "postal": "28013"},
    {"id": 11, "type": "COMPANY",    "name": "Nordic Tech ApS",              "city": "Copenhagen",  "country": "Denmark",        "postal": "1620"},
    {"id": 12, "type": "COMPANY",    "name": "O'Brien Industrial Ltd",       "city": "Dublin",      "country": "Ireland",        "postal": "D02"},
    {"id": 13, "type": "COMPANY",    "name": "Ionescu Logistics SRL",        "city": "Bucharest",   "country": "Romania",        "postal": "010071"},
    {"id": 14, "type": "COMPANY",    "name": "Silva & Filhos Lda",           "city": "Lisbon",      "country": "Portugal",       "postal": "1250"},
    {"id": 15, "type": "INDIVIDUAL", "name": "Erik Andersson",               "city": "Stockholm",   "country": "Sweden",         "postal": "11120"},
    {"id": 16, "type": "INDIVIDUAL", "name": "Aino Virtanen",                "city": "Helsinki",    "country": "Finland",        "postal": "00100"},
    {"id": 17, "type": "COMPANY",    "name": "Brasserie Dubois SPRL",        "city": "Brussels",    "country": "Belgium",        "postal": "1000"},
    {"id": 18, "type": "COMPANY",    "name": "Laurent Mécanique SA",         "city": "Lyon",        "country": "France",         "postal": "69002"},
    {"id": 19, "type": "COMPANY",    "name": "Müller Werkzeuge AG",          "city": "Munich",      "country": "Germany",        "postal": "80331"},
    {"id": 20, "type": "INDIVIDUAL", "name": "Dimitar Petrov",               "city": "Sofia",       "country": "Bulgaria",       "postal": "1000"},
    {"id": 21, "type": "COMPANY",    "name": "Janssen Elektro BV",           "city": "Rotterdam",   "country": "Netherlands",    "postal": "3011"},
    {"id": 22, "type": "COMPANY",    "name": "Rossi Automazione S.p.A.",     "city": "Rome",        "country": "Italy",          "postal": "00187"},
    {"id": 23, "type": "COMPANY",    "name": "García Motor SL",              "city": "Barcelona",   "country": "Spain",          "postal": "08001"},
    {"id": 24, "type": "COMPANY",    "name": "Larsen Marine A/S",            "city": "Copenhagen",  "country": "Denmark",        "postal": "1260"},
    {"id": 25, "type": "COMPANY",    "name": "McCarthy Engineering",         "city": "Dublin",      "country": "Ireland",        "postal": "D02"},
    {"id": 26, "type": "COMPANY",    "name": "Popescu Medical SRL",          "city": "Bucharest",   "country": "Romania",        "postal": "030167"},
    {"id": 27, "type": "COMPANY",    "name": "Costa Industria Lda",          "city": "Porto",       "country": "Portugal",       "postal": "4000"},
    {"id": 28, "type": "INDIVIDUAL", "name": "Nils Eriksson",                "city": "Gothenburg",  "country": "Sweden",         "postal": "41101"},
    {"id": 29, "type": "INDIVIDUAL", "name": "Matti Koskinen",               "city": "Tampere",     "country": "Finland",        "postal": "33100"},
    {"id": 30, "type": "COMPANY",    "name": "De Smet Packaging NV",         "city": "Antwerp",     "country": "Belgium",        "postal": "2000"},
    {"id": 31, "type": "COMPANY",    "name": "Weber Precision GmbH",         "city": "Stuttgart",   "country": "Germany",        "postal": "70173"},
    {"id": 32, "type": "COMPANY",    "name": "Novak Strojírenství s.r.o.",   "city": "Prague",      "country": "Czech Republic", "postal": "11000"},
    {"id": 33, "type": "COMPANY",    "name": "Horváth Gépgyár Kft.",         "city": "Budapest",    "country": "Hungary",        "postal": "1061"},
    {"id": 34, "type": "COMPANY",    "name": "Magnusson Shipping AB",        "city": "Malmö",       "country": "Sweden",         "postal": "21139"},
    {"id": 35, "type": "COMPANY",    "name": "Bernasconi SA",                "city": "Lugano",      "country": "Switzerland",    "postal": "6900"},
    {"id": 36, "type": "COMPANY",    "name": "Lefebvre Distribution",        "city": "Paris",       "country": "France",         "postal": "75009"},
    {"id": 37, "type": "COMPANY",    "name": "Dimitriou Trading SA",         "city": "Athens",      "country": "Greece",         "postal": "10563"},
    {"id": 38, "type": "COMPANY",    "name": "Kwiatkowski Budownictwo",      "city": "Krakow",      "country": "Poland",         "postal": "31-042"},
    {"id": 39, "type": "INDIVIDUAL", "name": "Anna Svensson",                "city": "Stockholm",   "country": "Sweden",         "postal": "11453"},
    {"id": 40, "type": "COMPANY",    "name": "Brennan Healthcare",           "city": "Dublin",      "country": "Ireland",        "postal": "D02"},
    {"id": 41, "type": "COMPANY",    "name": "Becker Optik GmbH",            "city": "Berlin",      "country": "Germany",        "postal": "10117"},
    {"id": 42, "type": "COMPANY",    "name": "Tóth Elektronika Kft.",        "city": "Budapest",    "country": "Hungary",        "postal": "1052"},
    {"id": 43, "type": "COMPANY",    "name": "Moretti Costruzioni",          "city": "Florence",    "country": "Italy",          "postal": "50123"},
    {"id": 44, "type": "COMPANY",    "name": "Lindqvist Energi AB",          "city": "Stockholm",   "country": "Sweden",         "postal": "11157"},
    {"id": 45, "type": "COMPANY",    "name": "Patel Import-Export Ltd",      "city": "London",      "country": "United Kingdom", "postal": "E14"},
    {"id": 46, "type": "COMPANY",    "name": "Krause Automotive GmbH",       "city": "Frankfurt",   "country": "Germany",        "postal": "60314"},
    {"id": 47, "type": "COMPANY",    "name": "Van Houten Chemicals BV",      "city": "The Hague",   "country": "Netherlands",    "postal": "2514"},
    {"id": 48, "type": "INDIVIDUAL", "name": "Olof Bergström",               "city": "Västerås",    "country": "Sweden",         "postal": "72212"},
    {"id": 49, "type": "COMPANY",    "name": "Schmidt Dental AG",            "city": "Zurich",      "country": "Switzerland",    "postal": "8001"},
    {"id": 50, "type": "COMPANY",    "name": "Kowalczyk Electronics",        "city": "Warsaw",      "country": "Poland",         "postal": "00-496"},
]

LOCATIONS = [
    {"id": 1,  "name": "Berlin Central Hub",          "type": "HUB",       "city": "Berlin",      "country": "Germany",        "lat": 52.52, "lng": 13.40},
    {"id": 2,  "name": "Munich Distribution Center",  "type": "WAREHOUSE", "city": "Munich",      "country": "Germany",        "lat": 48.14, "lng": 11.58},
    {"id": 3,  "name": "Frankfurt Airport Hub",       "type": "HUB",       "city": "Frankfurt",   "country": "Germany",        "lat": 50.04, "lng": 8.56},
    {"id": 4,  "name": "Hamburg Port Office",         "type": "OFFICE",    "city": "Hamburg",      "country": "Germany",        "lat": 53.55, "lng": 9.99},
    {"id": 5,  "name": "Paris North Hub",             "type": "HUB",       "city": "Paris",       "country": "France",         "lat": 48.86, "lng": 2.35},
    {"id": 6,  "name": "Amsterdam Sortation Center",  "type": "WAREHOUSE", "city": "Amsterdam",   "country": "Netherlands",    "lat": 52.37, "lng": 4.90},
    {"id": 7,  "name": "Vienna Last Mile Depot",      "type": "WAREHOUSE", "city": "Vienna",      "country": "Austria",        "lat": 48.21, "lng": 16.37},
    {"id": 8,  "name": "Zurich Pickup Point",         "type": "PICKUP_POINT","city": "Zurich",    "country": "Switzerland",    "lat": 47.38, "lng": 8.54},
    {"id": 9,  "name": "Warsaw Regional Hub",         "type": "HUB",       "city": "Warsaw",      "country": "Poland",         "lat": 52.23, "lng": 21.01},
    {"id": 10, "name": "Prague Office",               "type": "OFFICE",    "city": "Prague",      "country": "Czech Republic", "lat": 50.08, "lng": 14.44},
    {"id": 11, "name": "Milan South Hub",             "type": "HUB",       "city": "Milan",       "country": "Italy",          "lat": 45.46, "lng": 9.19},
    {"id": 12, "name": "Brussels Sorting Facility",   "type": "WAREHOUSE", "city": "Brussels",    "country": "Belgium",        "lat": 50.85, "lng": 4.35},
    {"id": 13, "name": "Copenhagen Nordic Hub",       "type": "HUB",       "city": "Copenhagen",  "country": "Denmark",        "lat": 55.68, "lng": 12.57},
    {"id": 14, "name": "Lyon Transit Center",         "type": "WAREHOUSE", "city": "Lyon",        "country": "France",         "lat": 45.76, "lng": 4.84},
    {"id": 15, "name": "Barcelona Port Terminal",     "type": "HUB",       "city": "Barcelona",   "country": "Spain",          "lat": 41.39, "lng": 2.17},
    {"id": 16, "name": "Stockholm Depot",             "type": "WAREHOUSE", "city": "Stockholm",   "country": "Sweden",         "lat": 59.33, "lng": 18.07},
    {"id": 17, "name": "Dublin Last Mile Center",     "type": "WAREHOUSE", "city": "Dublin",      "country": "Ireland",        "lat": 53.35, "lng": -6.26},
    {"id": 18, "name": "Bucharest Regional Hub",      "type": "HUB",       "city": "Bucharest",   "country": "Romania",        "lat": 44.43, "lng": 26.10},
    {"id": 19, "name": "Helsinki Pickup Station",     "type": "PICKUP_POINT","city": "Helsinki",  "country": "Finland",        "lat": 60.17, "lng": 24.94},
    {"id": 20, "name": "Lisbon West Office",          "type": "OFFICE",    "city": "Lisbon",      "country": "Portugal",       "lat": 38.72, "lng": -9.14},
]

SHIPPING_PRODUCTS = [
    {"id": 1, "name": "EU Standard Parcel",     "type": "STANDARD", "zone": "EU",       "base": 9.90,  "per_kg": 0.80, "max_kg": 30,  "days_min": 3, "days_max": 5, "pkg": "BOX"},
    {"id": 2, "name": "EU Express Parcel",      "type": "EXPRESS",  "zone": "EU",       "base": 24.90, "per_kg": 1.50, "max_kg": 30,  "days_min": 1, "days_max": 2, "pkg": "BOX"},
    {"id": 3, "name": "EU Economy Pallet",      "type": "ECONOMY",  "zone": "EU",       "base": 49.90, "per_kg": 0.30, "max_kg": 500, "days_min": 5, "days_max": 8, "pkg": "PALLET"},
    {"id": 4, "name": "EU Express Pallet",      "type": "EXPRESS",  "zone": "EU",       "base": 89.90, "per_kg": 0.60, "max_kg": 500, "days_min": 1, "days_max": 3, "pkg": "PALLET"},
    {"id": 5, "name": "Domestic Standard",      "type": "STANDARD", "zone": "DOMESTIC", "base": 5.90,  "per_kg": 0.50, "max_kg": 30,  "days_min": 2, "days_max": 3, "pkg": "BOX"},
    {"id": 6, "name": "Domestic Express",       "type": "EXPRESS",  "zone": "DOMESTIC", "base": 14.90, "per_kg": 1.00, "max_kg": 30,  "days_min": 1, "days_max": 1, "pkg": "BOX"},
    {"id": 7, "name": "EU Envelope",            "type": "STANDARD", "zone": "EU",       "base": 4.90,  "per_kg": 0.00, "max_kg": 2,   "days_min": 3, "days_max": 5, "pkg": "ENVELOPE"},
    {"id": 8, "name": "EU Express Envelope",    "type": "EXPRESS",  "zone": "EU",       "base": 12.90, "per_kg": 0.00, "max_kg": 2,   "days_min": 1, "days_max": 2, "pkg": "ENVELOPE"},
]

CARRIERS = ["DHL Express", "DB Schenker", "FedEx", "DPD", "GLS", "UPS", "Dachser", "PostNL", "Hermes", "Chronopost"]
CARD_BRANDS = ["VISA", "MASTERCARD", "AMEX", "MAESTRO"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
DECLARED_CONTENTS = [
    "Electronics components", "Lab equipment", "Office supplies", "Machine parts",
    "Medical devices", "Auto parts", "Textile samples", "Food supplements",
    "Construction materials", "Printed documents", "Ceramic goods", "Optical instruments",
    "Furniture parts", "Chemical samples", "Tools and hardware", "Books and media",
    "Replacement motors", "Sensor modules", "Valves and fittings", "Computer servers",
]
DRIVER_FIRST = ["Klaus", "Jean-Luc", "Pieter", "Marco", "Lars", "Tomáš", "Stefan", "Sean", "Andrei", "Erik", "Pierre", "Luca", "Jordi", "Martin", "Conor", "Willem"]
DRIVER_LAST = ["Richter", "Moreau", "Jansen", "Bianchi", "Pedersen", "Novák", "Gruber", "Murphy", "Marin", "Lindström", "Vanderstraeten", "Ferretti", "Puig", "Huber", "Ryan", "de Vries"]
EU_COUNTRIES = ["Germany", "France", "Netherlands", "Czech Republic", "Switzerland", "Poland", "Austria", "Italy", "Denmark", "Spain", "Belgium", "Sweden", "Ireland", "Romania", "Finland", "Portugal", "Hungary", "Bulgaria", "Greece", "United Kingdom"]
COUNTRY_IP_PREFIXES = {
    "Germany": "91.23", "France": "82.45", "Netherlands": "145.90", "Czech Republic": "78.128",
    "Switzerland": "178.82", "Poland": "185.45", "Austria": "77.116", "Italy": "151.38",
    "Denmark": "212.88", "Spain": "88.12", "Belgium": "109.23", "Sweden": "83.55",
    "Ireland": "86.45", "Romania": "79.114", "Finland": "91.152", "Portugal": "95.92",
    "Hungary": "84.206", "Bulgaria": "78.83", "Greece": "94.66", "United Kingdom": "81.174",
}

FRAUD_DEVICE_POOL = ["fp_fraud_000", "fp_fraud_001", "fp_fraud_002", "fp_fraud_003", "fp_fraud_004"]

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tms_producer_state.json")

_counters = {
    "order": 10,
    "order_item": 11,
    "payment": 10,
    "package": 10,
    "event": 57,
    "delivery": 8,
}


def load_state():
    """Load ID counters from the local state file.

    If the file exists, overwrites the in-memory counters with the saved
    values so that subsequent runs continue from where the last run left
    off, avoiding duplicate IDs.
    """
    global _counters
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            saved = json.load(f)
        _counters.update(saved)
        print(f"Loaded state from {STATE_FILE}: {_counters}")


def save_state():
    """Persist current ID counters to the local state file.

    Called after each order is fully produced so that a restart resumes
    from the correct IDs.
    """
    with open(STATE_FILE, "w") as f:
        json.dump(_counters, f)


def next_id(name):
    """Generate an auto-incrementing ID for a given entity type.

    Counters start after the last ID used in tms_seed_data.sql (or the
    last saved state) to avoid collisions. State is kept in memory and
    flushed to disk via save_state().

    Args:
        name: Entity type — one of 'order', 'order_item', 'payment',
              'package', 'event', 'delivery'.

    Returns:
        The next sequential integer ID for the given entity.
    """
    _counters[name] += 1
    return _counters[name]


def haversine(lat1, lng1, lat2, lng2):
    """Calculate the great-circle distance between two points on Earth.

    Uses the Haversine formula to compute distance in kilometres from
    latitude/longitude coordinates.

    Args:
        lat1: Latitude of point 1 in decimal degrees.
        lng1: Longitude of point 1 in decimal degrees.
        lat2: Latitude of point 2 in decimal degrees.
        lng2: Longitude of point 2 in decimal degrees.

    Returns:
        Distance in kilometres (float).
    """
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return r * 2 * math.asin(math.sqrt(a))


def nearest_hub(city, country, exclude_ids=None):
    """Find the closest logistics hub or warehouse to a given city.

    Looks up the city in LOCATIONS for coordinates, then returns the
    nearest HUB or WAREHOUSE by haversine distance. Falls back to a
    random HUB if the city is not found.

    Args:
        city: City name to search near.
        country: Country name (used to disambiguate cities).
        exclude_ids: Optional set of location IDs to skip (e.g. to avoid
                     picking the same hub as origin and destination).

    Returns:
        A location dict from the LOCATIONS list.
    """
    exclude_ids = exclude_ids or set()
    target = next((c for c in CUSTOMERS if c["city"] == city and c["country"] == country), None)
    if not target:
        target_loc = next((l for l in LOCATIONS if l["city"] == city), None)
        if target_loc:
            tlat, tlng = target_loc["lat"], target_loc["lng"]
        else:
            return random.choice([l for l in LOCATIONS if l["type"] == "HUB"])
    else:
        matching_loc = next((l for l in LOCATIONS if l["city"] == city), None)
        if matching_loc:
            tlat, tlng = matching_loc["lat"], matching_loc["lng"]
        else:
            tlat, tlng = 50.0, 10.0

    candidates = [l for l in LOCATIONS if l["id"] not in exclude_ids and l["type"] in ("HUB", "WAREHOUSE")]
    if not candidates:
        candidates = [l for l in LOCATIONS if l["id"] not in exclude_ids]
    return min(candidates, key=lambda l: haversine(tlat, tlng, l["lat"], l["lng"]))


def build_route(origin_loc, dest_loc):
    """Build a multi-hop delivery route between two locations.

    Adds 0-2 intermediate HUB stops depending on the direct distance:
    - > 800 km: 1-2 intermediate hubs near the midpoint.
    - 400-800 km: 1 intermediate hub.
    - < 400 km: direct route (no intermediate stops).

    Args:
        origin_loc: Origin location dict (from LOCATIONS).
        dest_loc: Destination location dict (from LOCATIONS).

    Returns:
        Ordered list of location dicts representing the route.
    """
    if origin_loc["id"] == dest_loc["id"]:
        return [origin_loc]

    route = [origin_loc]
    hubs_only = [l for l in LOCATIONS if l["type"] == "HUB" and l["id"] not in (origin_loc["id"], dest_loc["id"])]
    mid = {"lat": (origin_loc["lat"] + dest_loc["lat"]) / 2, "lng": (origin_loc["lng"] + dest_loc["lng"]) / 2}
    direct_dist = haversine(origin_loc["lat"], origin_loc["lng"], dest_loc["lat"], dest_loc["lng"])

    if direct_dist > 800:
        nearby = sorted(hubs_only, key=lambda h: haversine(mid["lat"], mid["lng"], h["lat"], h["lng"]))
        num_stops = min(random.randint(1, 2), len(nearby))
        for h in nearby[:num_stops]:
            route.append(h)
    elif direct_dist > 400:
        nearby = sorted(hubs_only, key=lambda h: haversine(mid["lat"], mid["lng"], h["lat"], h["lng"]))
        if nearby:
            route.append(nearby[0])

    route.append(dest_loc)
    return route


def random_ip(country):
    """Generate a plausible IP address for a given European country.

    Uses a fixed prefix per country from COUNTRY_IP_PREFIXES and
    randomises the last two octets.

    Args:
        country: Country name (e.g. 'Germany').

    Returns:
        IPv4 address string (e.g. '91.23.142.55').
    """
    prefix = COUNTRY_IP_PREFIXES.get(country, "10.0")
    return f"{prefix}.{random.randint(1,254)}.{random.randint(1,254)}"


def generate_order(is_fraud=False, return_rate=0.02, start_date=None):
    """Generate a complete order lifecycle with all related entities.

    Produces a realistic end-to-end order: picks a random sender and
    recipient from CUSTOMERS, selects a shipping product, builds a
    multi-hop route, and generates the full chain of records:
    ORDER → ORDER_ITEMS → PAYMENT → PACKAGES → TRACKING_EVENTS → DELIVERY.

    Tracking events follow chronological order through hub stops with
    realistic time gaps. Optionally injects:
    - Fraud signals into the payment (when is_fraud=True).
    - 2% transit delays (3-7 day EXCEPTION events).
    - Configurable package returns (RETURN_INITIATED → reverse route → RETURNED).

    Args:
        is_fraud: If True, inject one fraud pattern into the payment:
                  billing_mismatch, foreign_ip, device_reuse, or value_inflation.
        return_rate: Probability (0.0-1.0) that each package is returned
                     due to damage or delivery failure.

    Returns:
        Dict with keys: 'order', 'items', 'payment', 'packages',
        'tracking_events', 'deliveries', 'fraud_type'.
    """
    now = start_date if start_date else datetime.now(timezone.utc)
    customer = random.choice(CUSTOMERS)
    dest_customer = random.choice([c for c in CUSTOMERS if c["id"] != customer["id"]])

    is_domestic = customer["country"] == dest_customer["country"]
    if is_domestic:
        eligible = [p for p in SHIPPING_PRODUCTS if p["zone"] == "DOMESTIC"]
    else:
        eligible = [p for p in SHIPPING_PRODUCTS if p["zone"] == "EU"]
    product = random.choice(eligible)

    origin_loc = nearest_hub(customer["city"], customer["country"])
    dest_loc = nearest_hub(dest_customer["city"], dest_customer["country"], exclude_ids={origin_loc["id"]})
    route = build_route(origin_loc, dest_loc)

    order_id = next_id("order")
    num_items = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
    weight = round(random.uniform(0.5, min(product["max_kg"], 50)), 2)
    unit_price = round(product["base"] + product["per_kg"] * weight, 2)
    total = round(unit_price * num_items, 2)
    est_days = random.randint(product["days_min"], product["days_max"])

    order = {
        "order_id": order_id,
        "customer_id": customer["id"],
        "order_date": now.isoformat(),
        "status": "CONFIRMED",
        "origin_location_id": origin_loc["id"],
        "pickup_address": customer.get("address", f"Street {random.randint(1,200)}"),
        "pickup_city": customer["city"],
        "pickup_country": customer["country"],
        "destination_address": f"Street {random.randint(1,200)}",
        "destination_city": dest_customer["city"],
        "destination_country": dest_customer["country"],
        "estimated_delivery_date": (now + timedelta(days=est_days)).strftime("%Y-%m-%d"),
        "total_amount": total,
        "currency": "EUR",
    }

    items = []
    packages = []
    for i in range(num_items):
        item_id = next_id("order_item")
        declared_value = round(random.uniform(50, 5000), 2)
        item = {
            "order_item_id": item_id,
            "order_id": order_id,
            "product_id": product["id"],
            "quantity": 1,
            "unit_price": unit_price,
            "declared_weight_kg": weight,
            "declared_contents": random.choice(DECLARED_CONTENTS),
            "declared_value": declared_value,
            "insurance_opted": random.random() < 0.3,
        }
        items.append(item)

        pkg_id = next_id("package")
        pkg = {
            "package_id": pkg_id,
            "order_item_id": item_id,
            "order_id": order_id,
            "tracking_number": f"TMS-{now.strftime('%Y')}-{pkg_id:06d}",
            "actual_weight_kg": weight,
            "length_cm": round(random.uniform(10, 120), 1),
            "width_cm": round(random.uniform(10, 80), 1),
            "height_cm": round(random.uniform(5, 60), 1),
            "package_type": product["pkg"],
            "status": "PACKED",
            "created_at": now.isoformat(),
        }
        packages.append(pkg)

    fraud_type = None
    pay_method = random.choice(PAYMENT_METHODS)
    card_brand = random.choice(CARD_BRANDS) if pay_method in ("CREDIT_CARD", "DEBIT_CARD") else None
    card_last = f"{random.randint(1000,9999)}" if card_brand else None
    card_country = customer["country"]
    billing_country = customer["country"]
    ip = random_ip(customer["country"])
    fp = f"fp_{uuid.uuid4().hex[:8]}"

    if is_fraud:
        fraud_type = random.choice(["billing_mismatch", "foreign_ip", "device_reuse", "value_inflation"])
        if fraud_type == "billing_mismatch":
            billing_country = random.choice([c for c in EU_COUNTRIES if c != customer["country"]])
            card_country = billing_country
        elif fraud_type == "foreign_ip":
            foreign = random.choice([c for c in EU_COUNTRIES if c != customer["country"]])
            ip = random_ip(foreign)
        elif fraud_type == "device_reuse":
            fp = random.choice(FRAUD_DEVICE_POOL)
        elif fraud_type == "value_inflation":
            for item in items:
                item["declared_value"] = round(item["declared_value"] * random.uniform(15, 50), 2)

    payment = {
        "payment_id": next_id("payment"),
        "order_id": order_id,
        "payment_method": pay_method,
        "card_last_four": card_last,
        "card_brand": card_brand,
        "card_country": card_country,
        "billing_address": f"Street {random.randint(1,200)}",
        "billing_city": customer["city"],
        "billing_country": billing_country,
        "payment_amount": total,
        "currency": "EUR",
        "payment_status": "COMPLETED",
        "payment_timestamp": (now + timedelta(seconds=random.randint(10, 60))).isoformat(),
        "ip_address": ip,
        "device_fingerprint": fp,
        "transaction_reference": f"TXN-{now.strftime('%Y')}-{order_id:06d}",
    }

    carrier = random.choice(CARRIERS)
    tracking_events = []
    deliveries = []
    t = now + timedelta(hours=random.uniform(1, 3))
    delay_reasons = ["Customs clearance delay", "Severe weather disruption", "Hub capacity overflow", "Vehicle breakdown", "Security inspection hold", "Mislabeled package rerouted"]
    return_reasons = ["Package damaged during transit", "Recipient refused delivery", "Failed delivery attempt - address not found", "Failed delivery attempt - nobody home", "Damaged packaging, contents exposed", "Incorrect recipient details"]

    for pkg in packages:
        is_delayed = random.random() < 0.02
        is_returned = random.random() < return_rate
        delay_injected = False
        for idx, loc in enumerate(route):
            if idx == 0:
                eid = next_id("event")
                tracking_events.append({
                    "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                    "event_timestamp": t.isoformat(), "event_type": "PICKED_UP",
                    "status": "IN_TRANSIT", "description": f"Picked up at {loc['name']}", "carrier": carrier,
                })
                t += timedelta(hours=random.uniform(4, 8))
                eid = next_id("event")
                tracking_events.append({
                    "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                    "event_timestamp": t.isoformat(), "event_type": "DEPARTED_HUB",
                    "status": "IN_TRANSIT", "description": f"Departed {loc['name']}", "carrier": carrier,
                })
                if is_delayed and not delay_injected:
                    delay_days = random.uniform(3, 7)
                    t += timedelta(days=delay_days)
                    eid = next_id("event")
                    tracking_events.append({
                        "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                        "event_timestamp": t.isoformat(), "event_type": "EXCEPTION",
                        "status": "DELAYED", "description": random.choice(delay_reasons), "carrier": carrier,
                    })
                    delay_injected = True
            elif idx == len(route) - 1:
                t += timedelta(hours=random.uniform(6, 14))
                eid = next_id("event")
                tracking_events.append({
                    "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                    "event_timestamp": t.isoformat(), "event_type": "ARRIVED_AT_HUB",
                    "status": "IN_TRANSIT", "description": f"Arrived at {loc['name']}", "carrier": carrier,
                })

                if is_returned:
                    return_reason = random.choice(return_reasons)
                    t += timedelta(hours=random.uniform(8, 16))
                    eid = next_id("event")
                    tracking_events.append({
                        "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                        "event_timestamp": t.isoformat(), "event_type": "EXCEPTION",
                        "status": "RETURN_INITIATED", "description": return_reason, "carrier": carrier,
                    })
                    for ret_loc in reversed(route[:-1]):
                        t += timedelta(hours=random.uniform(8, 16))
                        eid = next_id("event")
                        tracking_events.append({
                            "event_id": eid, "package_id": pkg["package_id"], "location_id": ret_loc["id"],
                            "event_timestamp": t.isoformat(), "event_type": "ARRIVED_AT_HUB",
                            "status": "RETURNING", "description": f"Return via {ret_loc['name']}", "carrier": carrier,
                        })
                    t += timedelta(hours=random.uniform(2, 6))
                    eid = next_id("event")
                    tracking_events.append({
                        "event_id": eid, "package_id": pkg["package_id"], "location_id": route[0]["id"],
                        "event_timestamp": t.isoformat(), "event_type": "RETURNED",
                        "status": "RETURNED", "description": f"Returned to sender at {route[0]['name']}", "carrier": carrier,
                    })
                    pkg["status"] = "RETURNED"

                    did = next_id("delivery")
                    deliveries.append({
                        "delivery_id": did, "package_id": pkg["package_id"],
                        "driver_name": None, "vehicle_id": None,
                        "scheduled_date": (now + timedelta(days=est_days)).strftime("%Y-%m-%d"),
                        "actual_delivery_date": None,
                        "recipient_name": dest_customer["name"],
                        "signature_collected": False,
                        "delivery_notes": f"RETURNED: {return_reason}",
                        "status": "FAILED",
                    })
                else:
                    t += timedelta(hours=random.uniform(8, 16))
                    eid = next_id("event")
                    tracking_events.append({
                        "event_id": eid, "package_id": pkg["package_id"], "location_id": None,
                        "event_timestamp": t.isoformat(), "event_type": "OUT_FOR_DELIVERY",
                        "status": "IN_TRANSIT", "description": "With delivery driver", "carrier": carrier,
                    })
                    t += timedelta(hours=random.uniform(1, 4))
                    eid = next_id("event")
                    tracking_events.append({
                        "event_id": eid, "package_id": pkg["package_id"], "location_id": None,
                        "event_timestamp": t.isoformat(), "event_type": "DELIVERED",
                        "status": "DELIVERED", "description": f"Delivered to {dest_customer['city']}", "carrier": carrier,
                    })
                    pkg["status"] = "DELIVERED"

                    did = next_id("delivery")
                    driver_name = f"{random.choice(DRIVER_FIRST)} {random.choice(DRIVER_LAST)}"
                    deliveries.append({
                        "delivery_id": did, "package_id": pkg["package_id"],
                        "driver_name": driver_name,
                        "vehicle_id": f"VAN-{dest_customer['country'][:2].upper()}-{random.randint(1,99):03d}",
                        "scheduled_date": (now + timedelta(days=est_days)).strftime("%Y-%m-%d"),
                        "actual_delivery_date": t.isoformat(),
                        "recipient_name": dest_customer["name"],
                        "signature_collected": random.random() < 0.7,
                        "delivery_notes": "Delivered successfully",
                        "status": "COMPLETED",
                    })
            else:
                t += timedelta(hours=random.uniform(6, 12))
                eid = next_id("event")
                tracking_events.append({
                    "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                    "event_timestamp": t.isoformat(), "event_type": "ARRIVED_AT_HUB",
                    "status": "IN_TRANSIT", "description": f"Arrived at {loc['name']}", "carrier": carrier,
                })
                t += timedelta(hours=random.uniform(2, 4))
                eid = next_id("event")
                tracking_events.append({
                    "event_id": eid, "package_id": pkg["package_id"], "location_id": loc["id"],
                    "event_timestamp": t.isoformat(), "event_type": "DEPARTED_HUB",
                    "status": "IN_TRANSIT", "description": f"Departed {loc['name']}", "carrier": carrier,
                })

    has_returned = any(p["status"] == "RETURNED" for p in packages)
    all_delivered = all(p["status"] == "DELIVERED" for p in packages)
    if has_returned:
        order["status"] = "RETURNED"
    elif all_delivered:
        order["status"] = "DELIVERED"
    else:
        order["status"] = "PARTIALLY_DELIVERED"

    return {
        "order": order,
        "items": items,
        "payment": payment,
        "packages": packages,
        "tracking_events": tracking_events,
        "deliveries": deliveries,
        "fraud_type": fraud_type,
    }

def main():
    """CLI entry point — parse args, connect to Kafka, and run the producer loop.

    On startup, loads ID counters from .tms_producer_state.json (if it exists)
    so that subsequent runs continue with unique IDs. After each order is
    fully produced, the counters are flushed back to disk.

    Continuously generates orders at the configured rate, runs fraud
    detection on each payment, enriches the payment JSON with
    fraud_score/fraud_signals, and publishes all entities to their
    respective Kafka topics.

    Supports --dry-run mode which prints JSON to stdout instead of
    sending to Kafka, and --reset-state to discard saved counters.
    """
    parser = argparse.ArgumentParser(description="TMS Order Producer for Kafka/Redpanda")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--username", default=os.getenv("KAFKA_USERNAME", "kafka_user"))
    parser.add_argument("--password", default=os.getenv("KAFKA_PASSWORD", "kafka_pass"))
    parser.add_argument("--topic-prefix", default=os.getenv("KAFKA_TOPIC_PREFIX", "tms"))
    parser.add_argument("--count", type=int, default=int(os.getenv("TMS_ORDER_COUNT", "1")))
    parser.add_argument("--delay", type=float, default=float(os.getenv("TMS_DELAY", "1.0")))
    parser.add_argument("--fraud-rate", type=float, default=float(os.getenv("TMS_FRAUD_RATE", "0.01")))
    parser.add_argument("--return-rate", type=float, default=float(os.getenv("TMS_RETURN_RATE", "0.02")), help="Fraction of packages returned (default 0.02 = 2%%)")
    parser.add_argument("--dry-run", action="store_true", help="Print JSON instead of sending to Kafka")
    parser.add_argument("--reset-state", action="store_true", help="Delete saved state and start from seed IDs")
    parser.add_argument("--days-back", type=int, default=int(os.getenv("TMS_DAYS_BACK", "0")), help="Spread orders over the past N days (default: 0)")
    args = parser.parse_args()

    print(f"Configuration:")
    print(f"  bootstrap-servers: {args.bootstrap_servers}")
    print(f"  username:          {args.username}")
    print(f"  topic-prefix:      {args.topic_prefix}")
    print(f"  count:             {args.count}")
    print(f"  delay:             {args.delay}s")
    print(f"  fraud-rate:        {args.fraud_rate}")
    print(f"  return-rate:       {args.return_rate}")
    print(f"  days-back:         {args.days_back}")
    print(f"  dry-run:           {args.dry_run}")
    print()

    if args.reset_state and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"State file deleted: {STATE_FILE}")

    load_state()

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=args.username,
            sasl_plain_password=args.password,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            api_version=(2, 8, 0),
            request_timeout_ms=30000,
            max_block_ms=30000,
        )

    topics = {
        "orders":          f"{args.topic_prefix}-orders",
        "order_items":     f"{args.topic_prefix}-order-items",
        "payments":        f"{args.topic_prefix}-payments",
        "packages":        f"{args.topic_prefix}-packages",
        "tracking_events": f"{args.topic_prefix}-tracking-events",
        "deliveries":      f"{args.topic_prefix}-deliveries",
    }

    print(f"TMS Producer | count={args.count} | delay={args.delay}s | fraud={args.fraud_rate*100:.0f}% | returns={args.return_rate*100:.0f}%")
    print(f"Topics: {', '.join(topics.values())}")
    if args.dry_run:
        print("** DRY RUN — printing JSON only **")
    print()

    def send(topic, key, value):
        if args.dry_run:
            print(f"  [{topic}] key={key} → {json.dumps(value, default=str)[:200]}...")
        else:
            future = producer.send(topic, key=key, value=value)
            try:
                metadata = future.get(timeout=10)
            except Exception as e:
                print(f"  ERROR sending to {topic} key={key}: {e}")
                raise

    total_messages = 0
    for i in range(args.count):
        is_fraud = random.random() < args.fraud_rate
        order_time = datetime.now(timezone.utc) - timedelta(
            seconds=random.uniform(0, args.days_back * 86400)
        )
        result = generate_order(is_fraud=is_fraud, return_rate=args.return_rate, start_date=order_time)
        order_key = str(result["order"]["order_id"])
        fraud_tag = f" ** FRAUD: {result['fraud_type']} **" if result["fraud_type"] else ""

        send(topics["orders"], order_key, result["order"])
        total_messages += 1

        for item in result["items"]:
            send(topics["order_items"], order_key, item)
            total_messages += 1

        send(topics["payments"], order_key, result["payment"])
        total_messages += 1

        for pkg in result["packages"]:
            send(topics["packages"], order_key, pkg)
            total_messages += 1

        for evt in result["tracking_events"]:
            send(topics["tracking_events"], order_key, evt)
            total_messages += 1

        for dlv in result["deliveries"]:
            send(topics["deliveries"], order_key, dlv)
            total_messages += 1

        print(f"[{i+1}/{args.count}] order={order_key} | items={len(result['items'])} | events={len(result['tracking_events'])} | msgs={total_messages}{fraud_tag}")

        if not args.dry_run and producer:
            producer.flush()

        save_state()

        time.sleep(args.delay)

    if producer:
        producer.flush()
        producer.close()

    print(f"\nDone. Produced {args.count} orders ({total_messages} total messages).")


if __name__ == "__main__":
    main()
