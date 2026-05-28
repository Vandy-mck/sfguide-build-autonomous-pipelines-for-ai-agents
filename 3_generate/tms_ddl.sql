/* Architecture diagram

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

*/

CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_TYPE VARCHAR NOT NULL,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    COMPANY_NAME VARCHAR,
    TAX_ID VARCHAR,
    EMAIL VARCHAR NOT NULL,
    PHONE VARCHAR,
    ADDRESS VARCHAR,
    CITY VARCHAR,
    POSTAL_CODE VARCHAR,
    COUNTRY VARCHAR,
    PREFERRED_LANGUAGE VARCHAR DEFAULT 'EN',
    ACCOUNT_STATUS VARCHAR DEFAULT 'ACTIVE',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SHIPPING_PRODUCTS (
    PRODUCT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NAME VARCHAR NOT NULL,
    DESCRIPTION VARCHAR,
    SERVICE_TYPE VARCHAR NOT NULL,
    MAX_WEIGHT_KG NUMBER(10,2),
    MAX_LENGTH_CM NUMBER(10,2),
    MAX_WIDTH_CM NUMBER(10,2),
    MAX_HEIGHT_CM NUMBER(10,2),
    ZONE VARCHAR NOT NULL,
    BASE_PRICE NUMBER(10,2) NOT NULL,
    PRICE_PER_KG NUMBER(10,2) DEFAULT 0,
    ESTIMATED_DAYS_MIN NUMBER,
    ESTIMATED_DAYS_MAX NUMBER,
    INSURANCE_INCLUDED BOOLEAN DEFAULT FALSE,
    SIGNATURE_REQUIRED BOOLEAN DEFAULT FALSE,
    ACTIVE BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE LOCATIONS (
    LOCATION_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NAME VARCHAR NOT NULL,
    TYPE VARCHAR NOT NULL,
    ADDRESS VARCHAR,
    CITY VARCHAR,
    COUNTRY VARCHAR,
    LATITUDE NUMBER(10,7),
    LONGITUDE NUMBER(10,7),
    CAPACITY NUMBER,
    OPERATING_HOURS VARCHAR,
    ACTIVE BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID NUMBER NOT NULL,
    ORDER_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS VARCHAR DEFAULT 'PENDING',
    ORIGIN_LOCATION_ID NUMBER,
    PICKUP_ADDRESS VARCHAR,
    PICKUP_CITY VARCHAR,
    PICKUP_COUNTRY VARCHAR,
    DESTINATION_ADDRESS VARCHAR,
    DESTINATION_CITY VARCHAR,
    DESTINATION_COUNTRY VARCHAR,
    ESTIMATED_DELIVERY_DATE DATE,
    TOTAL_AMOUNT NUMBER(10,2),
    CURRENCY VARCHAR DEFAULT 'EUR'
);

CREATE OR REPLACE TABLE ORDER_ITEMS (
    ORDER_ITEM_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID NUMBER NOT NULL,
    PRODUCT_ID NUMBER NOT NULL,
    QUANTITY NUMBER NOT NULL,
    UNIT_PRICE NUMBER(10,2),
    DECLARED_WEIGHT_KG NUMBER(10,2),
    DECLARED_CONTENTS VARCHAR,
    DECLARED_VALUE NUMBER(10,2),
    INSURANCE_OPTED BOOLEAN DEFAULT FALSE
);

CREATE OR REPLACE TABLE PAYMENTS (
    PAYMENT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID NUMBER NOT NULL,
    PAYMENT_METHOD VARCHAR NOT NULL,
    CARD_LAST_FOUR VARCHAR(4),
    CARD_BRAND VARCHAR,
    CARD_COUNTRY VARCHAR,
    BILLING_ADDRESS VARCHAR,
    BILLING_CITY VARCHAR,
    BILLING_COUNTRY VARCHAR,
    PAYMENT_AMOUNT NUMBER(10,2) NOT NULL,
    CURRENCY VARCHAR DEFAULT 'EUR',
    PAYMENT_STATUS VARCHAR DEFAULT 'PENDING',
    PAYMENT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IP_ADDRESS VARCHAR,
    DEVICE_FINGERPRINT VARCHAR,
    TRANSACTION_REFERENCE VARCHAR
);

CREATE OR REPLACE TABLE PACKAGES (
    PACKAGE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ITEM_ID NUMBER NOT NULL,
    ORDER_ID NUMBER NOT NULL,
    TRACKING_NUMBER VARCHAR NOT NULL,
    ACTUAL_WEIGHT_KG NUMBER(10,2),
    LENGTH_CM NUMBER(10,2),
    WIDTH_CM NUMBER(10,2),
    HEIGHT_CM NUMBER(10,2),
    PACKAGE_TYPE VARCHAR DEFAULT 'BOX',
    STATUS VARCHAR DEFAULT 'PACKED',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TRACKING_EVENTS (
    EVENT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    PACKAGE_ID NUMBER NOT NULL,
    LOCATION_ID NUMBER,
    EVENT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENT_TYPE VARCHAR NOT NULL,
    STATUS VARCHAR,
    DESCRIPTION VARCHAR,
    CARRIER VARCHAR
);

CREATE OR REPLACE TABLE DELIVERIES (
    DELIVERY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    PACKAGE_ID NUMBER NOT NULL,
    DRIVER_NAME VARCHAR,
    VEHICLE_ID VARCHAR,
    SCHEDULED_DATE DATE,
    ACTUAL_DELIVERY_DATE TIMESTAMP_NTZ,
    RECIPIENT_NAME VARCHAR,
    SIGNATURE_COLLECTED BOOLEAN DEFAULT FALSE,
    DELIVERY_NOTES VARCHAR,
    STATUS VARCHAR DEFAULT 'SCHEDULED'
);
