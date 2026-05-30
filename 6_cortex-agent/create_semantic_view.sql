-- =============================================================================
-- Create Semantic View: TMS_SEMANTIC_VIEW
-- 
-- Provides a semantic layer over the TMS Analytics views for use with
-- Cortex Analyst and Cortex Agents. Defines dimensions, metrics, relationships,
-- and verified queries for natural language querying.
--
-- Prerequisites:
--   - SUMMIT_DB_DEV.ANALYTICS views must exist (deployed via DCM project)
--   - Role must have CREATE SEMANTIC VIEW on SUMMIT_DB_DEV.ANALYTICS
--   - Role must have SELECT on all referenced views
--
-- Usage:
--   snow sql -f 6_cortex_code/create_semantic_view.sql
-- =============================================================================

USE ROLE SUMMIT_ADMIN;
USE WAREHOUSE SUMMIT_WH;
USE DATABASE SUMMIT_DB_DEV;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE SEMANTIC VIEW SUMMIT_DB_DEV.ANALYTICS.TMS_SEMANTIC_VIEW

  TABLES (
    order_summary AS SUMMIT_DB_DEV.ANALYTICS.ORDER_SUMMARY
      PRIMARY KEY (ORDER_ID)
      WITH SYNONYMS ('orders', 'shipments', 'order details')
      COMMENT = 'Denormalized order view with customer info, item counts, package counts, delivery status, and order value for a pan-European shipping company',

    package_tracking AS SUMMIT_DB_DEV.ANALYTICS.PACKAGE_TRACKING
      PRIMARY KEY (PACKAGE_ID)
      WITH SYNONYMS ('packages', 'parcels', 'shipment tracking')
      COMMENT = 'Package-level tracking showing transit progress, number of hubs visited, transit duration in hours, and carrier information',

    package_hops AS SUMMIT_DB_DEV.ANALYTICS.PACKAGE_HOPS
      PRIMARY KEY (EVENT_ID)
      WITH SYNONYMS ('routing', 'journey', 'package journey', 'hop by hop')
      COMMENT = 'Hop-by-hop journey of each package through the European logistics network, showing each location visited and time between stops',

    location_activity AS SUMMIT_DB_DEV.ANALYTICS.LOCATION_ACTIVITY
      PRIMARY KEY (LOCATION_ID, ACTIVITY_DATE)
      WITH SYNONYMS ('hub activity', 'warehouse activity', 'location performance', 'throughput')
      COMMENT = 'Daily throughput metrics and processing time percentiles for each logistics location (hubs, warehouses, offices, pickup points)',

    fraud_detection AS SUMMIT_DB_DEV.ANALYTICS.FRAUD_DETECTION
      PRIMARY KEY (PAYMENT_ID)
      WITH SYNONYMS ('fraud', 'fraud alerts', 'suspicious payments', 'payment fraud')
      COMMENT = 'Payment fraud scoring results with 6 heuristic signals, enriched with customer, order, and payment details'
  )

  RELATIONSHIPS (
    package_tracking_to_orders AS
      package_tracking (ORDER_ID) REFERENCES order_summary,
    package_hops_to_tracking AS
      package_hops (PACKAGE_ID) REFERENCES package_tracking,
    fraud_to_orders AS
      fraud_detection (ORDER_ID) REFERENCES order_summary
  )

  FACTS (
    order_summary.ORDER_VALUE AS ORDER_VALUE
      COMMENT = 'Order value in EUR for aggregation',
    order_summary.TOTAL_ITEMS AS TOTAL_ITEMS
      COMMENT = 'Number of items in the order',
    order_summary.TOTAL_PACKAGES AS TOTAL_PACKAGES
      COMMENT = 'Number of packages in the order',
    order_summary.DAYS_SINCE_ORDER AS DAYS_SINCE_ORDER
      COMMENT = 'Days elapsed since the order was placed',
    package_tracking.TRANSIT_HOURS AS TRANSIT_HOURS
      COMMENT = 'Transit hours for a single package',
    package_tracking.HUBS_VISITED AS HUBS_VISITED
      COMMENT = 'Number of hubs visited by the package',
    location_activity.NUM_PACKAGES AS NUM_PACKAGES
      COMMENT = 'Number of packages processed at the location on this day',
    location_activity.PACKAGES_ARRIVED AS PACKAGES_ARRIVED
      COMMENT = 'Packages that arrived at this location on this day',
    location_activity.PACKAGES_DEPARTED AS PACKAGES_DEPARTED
      COMMENT = 'Packages that departed from this location on this day',
    location_activity.PROCESSING_P90_MINUTES AS PROCESSING_P90_MINUTES
      COMMENT = 'P90 processing time in minutes at this location',
    package_hops.MINUTES_SINCE_PREV_HOP AS MINUTES_SINCE_PREV_HOP
      COMMENT = 'Minutes elapsed since the previous hop',
    fraud_detection.FRAUD_SCORE AS FRAUD_SCORE
      COMMENT = 'Fraud score (0.0 to 1.0) assigned by the heuristic scorer',
    fraud_detection.PAYMENT_AMOUNT AS PAYMENT_AMOUNT
      COMMENT = 'Payment amount in EUR'
  )

  DIMENSIONS (
    order_summary.customer_name AS CUSTOMER_NAME
      WITH SYNONYMS = ('client', 'shipper', 'sender')
      COMMENT = 'Customer name (company name or full name of individual)',
    order_summary.customer_city AS CUSTOMER_CITY
      COMMENT = 'City where the customer is located',
    order_summary.customer_country AS CUSTOMER_COUNTRY
      WITH SYNONYMS = ('origin country')
      COMMENT = 'Country where the customer is located',
    order_summary.order_date AS ORDER_DATE
      COMMENT = 'Date when the shipping order was placed',
    order_summary.order_status AS ORDER_STATUS
      WITH SYNONYMS = ('status', 'shipment status')
      COMMENT = 'Current order status (PENDING, CONFIRMED, IN_TRANSIT, DELIVERED, CANCELLED)',
    order_summary.destination_city AS DESTINATION_CITY
      WITH SYNONYMS = ('delivery city', 'ship to city')
      COMMENT = 'City where the shipment is being delivered',
    order_summary.destination_country AS DESTINATION_COUNTRY
      WITH SYNONYMS = ('delivery country', 'ship to country')
      COMMENT = 'Country where the shipment is being delivered',

    package_tracking.tracking_number AS TRACKING_NUMBER
      WITH SYNONYMS = ('tracking code', 'parcel number')
      COMMENT = 'Unique tracking number for the package (format: TMS-YYYY-NNNNNN)',
    package_tracking.package_status AS PACKAGE_STATUS
      WITH SYNONYMS = ('parcel status')
      COMMENT = 'Current package status (PACKED, IN_TRANSIT, DELIVERED, RETURNED)',
    package_tracking.package_type AS PACKAGE_TYPE
      COMMENT = 'Type of package (BOX, ENVELOPE, PALLET, TUBE)',
    package_tracking.carrier AS CARRIER
      WITH SYNONYMS = ('shipping carrier', 'logistics provider')
      COMMENT = 'Carrier handling the package delivery',

    package_hops.hop_location_name AS LOCATION_NAME
      WITH SYNONYMS = ('hub name', 'stop name')
      COMMENT = 'Name of the logistics location at this hop',
    package_hops.hop_city AS LOCATION_CITY
      COMMENT = 'City of the logistics location at this hop',
    package_hops.hop_country AS LOCATION_COUNTRY
      COMMENT = 'Country of the logistics location at this hop',
    package_hops.hop_location_type AS LOCATION_TYPE
      COMMENT = 'Type of location at this hop (HUB, WAREHOUSE, OFFICE, PICKUP_POINT)',
    package_hops.hop_event_type AS EVENT_TYPE
      WITH SYNONYMS = ('tracking event')
      COMMENT = 'Type of tracking event (PICKED_UP, ARRIVED_AT_HUB, DEPARTED_HUB, OUT_FOR_DELIVERY, DELIVERED)',

    location_activity.facility_name AS LOCATION_NAME
      WITH SYNONYMS = ('hub', 'warehouse', 'facility')
      COMMENT = 'Name of the logistics facility',
    location_activity.facility_type AS LOCATION_TYPE
      COMMENT = 'Type of facility (HUB, WAREHOUSE, OFFICE, PICKUP_POINT)',
    location_activity.facility_city AS CITY
      COMMENT = 'City where the facility is located',
    location_activity.facility_country AS COUNTRY
      COMMENT = 'Country where the facility is located',
    location_activity.activity_date AS ACTIVITY_DATE
      COMMENT = 'Date of the activity measurement',

    fraud_detection.fraud_customer_name AS CUSTOMER_NAME
      COMMENT = 'Name of the customer associated with the flagged payment',
    fraud_detection.fraud_customer_country AS CUSTOMER_COUNTRY
      COMMENT = 'Country of the customer associated with the flagged payment',
    fraud_detection.payment_method AS PAYMENT_METHOD
      WITH SYNONYMS = ('payment type')
      COMMENT = 'Payment method used (CREDIT_CARD, DEBIT_CARD, BANK_TRANSFER, DIGITAL_WALLET)',
    fraud_detection.card_brand AS CARD_BRAND
      COMMENT = 'Credit/debit card brand (VISA, MASTERCARD, AMEX)',
    fraud_detection.card_country AS CARD_COUNTRY
      COMMENT = 'Country where the card was issued',
    fraud_detection.billing_country AS BILLING_COUNTRY
      COMMENT = 'Country of the billing address',
    fraud_detection.is_fraud AS IS_FRAUD
      COMMENT = 'Whether the payment was flagged as fraudulent (TRUE/FALSE). Threshold is fraud_score >= 0.30',
    fraud_detection.fraud_signals AS FRAUD_SIGNALS
      WITH SYNONYMS = ('fraud reasons', 'fraud indicators')
      COMMENT = 'Comma-separated list of triggered fraud signals (billing_country_mismatch, card_country_mismatch, ip_geolocation_mismatch, known_fraud_device, velocity_abuse, high_declared_value)',
    fraud_detection.payment_timestamp AS PAYMENT_TIMESTAMP
      COMMENT = 'Timestamp when the payment was processed'
  )

  METRICS (
    order_summary.total_orders AS COUNT(ORDER_ID)
      WITH SYNONYMS = ('number of orders', 'order count')
      COMMENT = 'Total number of shipping orders',
    order_summary.total_order_value AS SUM(ORDER_VALUE)
      WITH SYNONYMS = ('revenue', 'total revenue', 'GMV')
      COMMENT = 'Total value of all orders in EUR',
    order_summary.avg_order_value AS AVG(ORDER_VALUE)
      COMMENT = 'Average order value in EUR',
    order_summary.total_packages_sum AS SUM(TOTAL_PACKAGES)
      COMMENT = 'Total number of packages across all orders',
    order_summary.avg_days_since_order AS AVG(DAYS_SINCE_ORDER)
      COMMENT = 'Average number of days since order was placed',

    package_tracking.total_packages_tracked AS COUNT(PACKAGE_ID)
      WITH SYNONYMS = ('package count')
      COMMENT = 'Total number of packages being tracked',
    package_tracking.avg_hubs_visited AS AVG(HUBS_VISITED)
      COMMENT = 'Average number of hubs visited per package',
    package_tracking.avg_transit_hours AS AVG(TRANSIT_HOURS)
      WITH SYNONYMS = ('average delivery time')
      COMMENT = 'Average transit time in hours across all packages',
    package_tracking.max_transit_hours AS MAX(TRANSIT_HOURS)
      COMMENT = 'Maximum transit time in hours (longest delivery)',

    package_hops.total_hops AS COUNT(EVENT_ID)
      COMMENT = 'Total number of tracking events (hops) recorded',
    package_hops.avg_minutes_between_hops AS AVG(MINUTES_SINCE_PREV_HOP)
      COMMENT = 'Average time in minutes between consecutive hops',

    location_activity.total_packages_processed AS SUM(NUM_PACKAGES)
      WITH SYNONYMS = ('throughput', 'volume')
      COMMENT = 'Total number of packages processed at locations',
    location_activity.avg_p90_processing_minutes AS AVG(PROCESSING_P90_MINUTES)
      WITH SYNONYMS = ('processing time', 'dwell time')
      COMMENT = 'Average P90 processing time in minutes across locations',
    location_activity.total_packages_arrived AS SUM(PACKAGES_ARRIVED)
      COMMENT = 'Total packages that arrived at locations',
    location_activity.total_packages_departed AS SUM(PACKAGES_DEPARTED)
      COMMENT = 'Total packages that departed from locations',

    fraud_detection.total_payments_scored AS COUNT(PAYMENT_ID)
      COMMENT = 'Total number of payments that have been scored for fraud',
    fraud_detection.total_fraud_flagged AS COUNT(CASE WHEN IS_FRAUD THEN PAYMENT_ID END)
      WITH SYNONYMS = ('fraud count', 'fraudulent payments')
      COMMENT = 'Number of payments flagged as fraudulent',
    fraud_detection.avg_fraud_score AS AVG(FRAUD_SCORE)
      COMMENT = 'Average fraud score across all scored payments',
    fraud_detection.total_fraud_amount AS SUM(CASE WHEN IS_FRAUD THEN PAYMENT_AMOUNT ELSE 0 END)
      WITH SYNONYMS = ('fraud value', 'fraudulent amount')
      COMMENT = 'Total monetary amount of fraudulent payments in EUR'
  )

  COMMENT = 'Semantic view for the EuroShip Logistics TMS (Transportation Management System). Covers order management, real-time package tracking across 20 European hubs, location performance monitoring, and AI-powered payment fraud detection. Use this view to ask questions about orders, deliveries, transit times, hub throughput, and fraud alerts.'

  AI_SQL_GENERATION 'When querying dates, use CURRENT_DATE() for today. When the user asks about packages "in transit", filter on PACKAGE_STATUS = ''IN_TRANSIT''. When the user asks about "delayed" packages, look for packages where TRANSIT_HOURS exceeds the average. For fraud queries, IS_FRAUD = TRUE identifies flagged payments. Always ORDER BY relevant metrics DESC unless the user specifies otherwise. Round numeric results to 2 decimal places.'

  AI_QUESTION_CATEGORIZATION 'This semantic view answers questions about: (1) shipping orders and their status, (2) package tracking and delivery times, (3) logistics hub/location performance and throughput, (4) payment fraud detection and alerts. If a question is about topics outside logistics, shipping, or payments, respond that this view only covers TMS data. If the user asks about a specific time period without specifying dates, ask them to clarify the date range.'

  AI_VERIFIED_QUERIES (
    packages_in_transit AS (
      QUESTION 'Which packages are currently in transit and how many hubs have they visited?'
      VERIFIED_AT 1748275200
      ONBOARDING_QUESTION TRUE
      VERIFIED_BY '(STEWARD = data_engineering)'
      SQL 'SELECT
               pt.TRACKING_NUMBER,
               pt.CUSTOMER_NAME,
               pt.DESTINATION_CITY,
               pt.DESTINATION_COUNTRY,
               pt.HUBS_VISITED,
               pt.TRANSIT_HOURS,
               pt.CARRIER,
               pt.FIRST_SCAN,
               pt.LAST_SCAN
             FROM package_tracking AS pt
             WHERE pt.PACKAGE_STATUS = ''IN_TRANSIT''
             ORDER BY pt.HUBS_VISITED DESC, pt.TRANSIT_HOURS DESC'
    ),

    top_locations_by_processing_time AS (
      QUESTION 'Show me the top 5 locations with the highest P90 processing time today'
      VERIFIED_AT 1748275200
      ONBOARDING_QUESTION TRUE
      VERIFIED_BY '(STEWARD = data_engineering)'
      SQL 'SELECT
               la.LOCATION_NAME,
               la.LOCATION_TYPE,
               la.CITY,
               la.COUNTRY,
               la.NUM_PACKAGES,
               la.PROCESSING_P75_MINUTES,
               la.PROCESSING_P90_MINUTES,
               la.PROCESSING_P99_MINUTES
             FROM location_activity AS la
             WHERE la.ACTIVITY_DATE = CURRENT_DATE()
             ORDER BY la.PROCESSING_P90_MINUTES DESC NULLS LAST
             LIMIT 5'
    ),

    recent_fraud_alerts AS (
      QUESTION 'List all fraudulent payments detected in the last 7 days with their fraud signals and scores'
      VERIFIED_AT 1748275200
      ONBOARDING_QUESTION TRUE
      VERIFIED_BY '(STEWARD = data_engineering)'
      SQL 'SELECT
               fd.CUSTOMER_NAME,
               fd.CUSTOMER_COUNTRY,
               fd.PAYMENT_METHOD,
               fd.CARD_BRAND,
               fd.CARD_COUNTRY,
               fd.BILLING_COUNTRY,
               fd.PAYMENT_AMOUNT,
               fd.FRAUD_SCORE,
               fd.FRAUD_SIGNALS,
               fd.PAYMENT_TIMESTAMP
             FROM fraud_detection AS fd
             WHERE fd.IS_FRAUD = TRUE
               AND fd.PAYMENT_TIMESTAMP >= DATEADD(''day'', -7, CURRENT_TIMESTAMP())
             ORDER BY fd.FRAUD_SCORE DESC, fd.PAYMENT_TIMESTAMP DESC'
    )
  );

-- Grant access to the developer role
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SUMMIT_DB_DEV.ANALYTICS.TMS_SEMANTIC_VIEW
  TO ROLE SUMMIT_DEVELOPER_ROLE_DEV;
