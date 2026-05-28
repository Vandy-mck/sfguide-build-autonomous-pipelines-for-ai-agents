-- =============================================================================
-- TMS Seed Data — reference data matching tms_producer.py
-- Run after DCM DEPLOY
-- Load BEFORE starting the Kafka producer.
-- Insert order: LOCATIONS -> CUSTOMERS -> SHIPPING_PRODUCTS -> 1 example order
-- [Warning] Will truncate existing tables before seeding data
--
-- snow sql -f scripts/seed_data.sql --variable "env_suffix=_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE SUMMIT_INGEST_ROLE{{env_suffix}};
USE SCHEMA SUMMIT_DB{{env_suffix}}.RAW;
USE WAREHOUSE SUMMIT_WH{{env_suffix}};

TRUNCATE TABLE LOCATIONS;
TRUNCATE TABLE CUSTOMERS;
TRUNCATE TABLE SHIPPING_PRODUCTS;
TRUNCATE TABLE DELIVERIES;
TRUNCATE TABLE TRACKING_EVENTS;
TRUNCATE TABLE PACKAGES;
TRUNCATE TABLE PAYMENTS;
TRUNCATE TABLE ORDER_ITEMS;
TRUNCATE TABLE ORDERS;

-- LOCATIONS (20 European logistics locations)
INSERT INTO LOCATIONS (LOCATION_ID, NAME, TYPE, CITY, COUNTRY, LATITUDE, LONGITUDE) VALUES
(1,  'Berlin Central Hub',         'HUB',          'Berlin',      'Germany',        52.52,  13.40),
(2,  'Munich Distribution Center', 'WAREHOUSE',    'Munich',      'Germany',        48.14,  11.58),
(3,  'Frankfurt Airport Hub',      'HUB',          'Frankfurt',   'Germany',        50.04,   8.56),
(4,  'Hamburg Port Office',        'OFFICE',       'Hamburg',      'Germany',        53.55,   9.99),
(5,  'Paris North Hub',            'HUB',          'Paris',       'France',         48.86,   2.35),
(6,  'Amsterdam Sortation Center', 'WAREHOUSE',    'Amsterdam',   'Netherlands',    52.37,   4.90),
(7,  'Vienna Last Mile Depot',     'WAREHOUSE',    'Vienna',      'Austria',        48.21,  16.37),
(8,  'Zurich Pickup Point',        'PICKUP_POINT', 'Zurich',      'Switzerland',    47.38,   8.54),
(9,  'Warsaw Regional Hub',        'HUB',          'Warsaw',      'Poland',         52.23,  21.01),
(10, 'Prague Office',              'OFFICE',       'Prague',      'Czech Republic', 50.08,  14.44),
(11, 'Milan South Hub',            'HUB',          'Milan',       'Italy',          45.46,   9.19),
(12, 'Brussels Sorting Facility',  'WAREHOUSE',    'Brussels',    'Belgium',        50.85,   4.35),
(13, 'Copenhagen Nordic Hub',      'HUB',          'Copenhagen',  'Denmark',        55.68,  12.57),
(14, 'Lyon Transit Center',        'WAREHOUSE',    'Lyon',        'France',         45.76,   4.84),
(15, 'Barcelona Port Terminal',    'HUB',          'Barcelona',   'Spain',          41.39,   2.17),
(16, 'Stockholm Depot',            'WAREHOUSE',    'Stockholm',   'Sweden',         59.33,  18.07),
(17, 'Dublin Last Mile Center',    'WAREHOUSE',    'Dublin',      'Ireland',        53.35,  -6.26),
(18, 'Bucharest Regional Hub',     'HUB',          'Bucharest',   'Romania',        44.43,  26.10),
(19, 'Helsinki Pickup Station',    'PICKUP_POINT', 'Helsinki',    'Finland',        60.17,  24.94),
(20, 'Lisbon West Office',         'OFFICE',       'Lisbon',      'Portugal',       38.72,  -9.14);

-- CUSTOMERS — companies
INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_TYPE, COMPANY_NAME, CITY, POSTAL_CODE, COUNTRY, EMAIL, ACCOUNT_STATUS, CREATED_AT) VALUES
(1,  'COMPANY', 'Schneider Electronics GmbH',   'Berlin',      '10178',  'Germany',        'orders@company1.eu',  'ACTIVE', '2024-01-01 00:00:00'),
(3,  'COMPANY', 'Van der Berg Imports BV',       'Amsterdam',   '1015',   'Netherlands',    'orders@company3.eu',  'ACTIVE', '2024-01-01 00:00:00'),
(4,  'COMPANY', 'TechStart Solutions s.r.o.',    'Prague',      '11000',  'Czech Republic', 'orders@company4.eu',  'ACTIVE', '2024-01-01 00:00:00'),
(6,  'COMPANY', 'Kowalski Transport Sp. z o.o.', 'Warsaw',     '00-001', 'Poland',         'orders@company6.eu',  'ACTIVE', '2024-01-01 00:00:00'),
(8,  'COMPANY', 'Medici Pharma S.r.l.',          'Milan',      '20121',  'Italy',          'orders@company8.eu',  'ACTIVE', '2024-01-01 00:00:00'),
(10, 'COMPANY', 'García Electrónica SL',         'Madrid',     '28013',  'Spain',          'orders@company10.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(11, 'COMPANY', 'Nordic Tech ApS',               'Copenhagen', '1620',   'Denmark',        'orders@company11.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(12, 'COMPANY', 'O''Brien Industrial Ltd',       'Dublin',     'D02',    'Ireland',        'orders@company12.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(13, 'COMPANY', 'Ionescu Logistics SRL',         'Bucharest',  '010071', 'Romania',        'orders@company13.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(14, 'COMPANY', 'Silva & Filhos Lda',            'Lisbon',     '1250',   'Portugal',       'orders@company14.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(17, 'COMPANY', 'Brasserie Dubois SPRL',         'Brussels',   '1000',   'Belgium',        'orders@company17.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(18, 'COMPANY', 'Laurent Mécanique SA',          'Lyon',       '69002',  'France',         'orders@company18.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(19, 'COMPANY', 'Müller Werkzeuge AG',           'Munich',     '80331',  'Germany',        'orders@company19.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(21, 'COMPANY', 'Janssen Elektro BV',            'Rotterdam',  '3011',   'Netherlands',    'orders@company21.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(22, 'COMPANY', 'Rossi Automazione S.p.A.',      'Rome',       '00187',  'Italy',          'orders@company22.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(23, 'COMPANY', 'García Motor SL',               'Barcelona',  '08001',  'Spain',          'orders@company23.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(24, 'COMPANY', 'Larsen Marine A/S',             'Copenhagen', '1260',   'Denmark',        'orders@company24.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(25, 'COMPANY', 'McCarthy Engineering',          'Dublin',     'D02',    'Ireland',        'orders@company25.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(26, 'COMPANY', 'Popescu Medical SRL',           'Bucharest',  '030167', 'Romania',        'orders@company26.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(27, 'COMPANY', 'Costa Industria Lda',           'Porto',      '4000',   'Portugal',       'orders@company27.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(30, 'COMPANY', 'De Smet Packaging NV',          'Antwerp',    '2000',   'Belgium',        'orders@company30.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(31, 'COMPANY', 'Weber Precision GmbH',          'Stuttgart',  '70173',  'Germany',        'orders@company31.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(32, 'COMPANY', 'Novak Strojírenství s.r.o.',    'Prague',     '11000',  'Czech Republic', 'orders@company32.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(33, 'COMPANY', 'Horváth Gépgyár Kft.',          'Budapest',   '1061',   'Hungary',        'orders@company33.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(34, 'COMPANY', 'Magnusson Shipping AB',         'Malmö',      '21139',  'Sweden',         'orders@company34.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(35, 'COMPANY', 'Bernasconi SA',                 'Lugano',     '6900',   'Switzerland',    'orders@company35.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(36, 'COMPANY', 'Lefebvre Distribution',         'Paris',      '75009',  'France',         'orders@company36.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(37, 'COMPANY', 'Dimitriou Trading SA',          'Athens',     '10563',  'Greece',         'orders@company37.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(38, 'COMPANY', 'Kwiatkowski Budownictwo',       'Krakow',     '31-042', 'Poland',         'orders@company38.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(40, 'COMPANY', 'Brennan Healthcare',            'Dublin',     'D02',    'Ireland',        'orders@company40.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(41, 'COMPANY', 'Becker Optik GmbH',             'Berlin',     '10117',  'Germany',        'orders@company41.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(42, 'COMPANY', 'Tóth Elektronika Kft.',         'Budapest',   '1052',   'Hungary',        'orders@company42.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(43, 'COMPANY', 'Moretti Costruzioni',           'Florence',   '50123',  'Italy',          'orders@company43.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(44, 'COMPANY', 'Lindqvist Energi AB',           'Stockholm',  '11157',  'Sweden',         'orders@company44.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(45, 'COMPANY', 'Patel Import-Export Ltd',       'London',     'E14',    'United Kingdom', 'orders@company45.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(46, 'COMPANY', 'Krause Automotive GmbH',        'Frankfurt',  '60314',  'Germany',        'orders@company46.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(47, 'COMPANY', 'Van Houten Chemicals BV',       'The Hague',  '2514',   'Netherlands',    'orders@company47.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(49, 'COMPANY', 'Schmidt Dental AG',             'Zurich',     '8001',   'Switzerland',    'orders@company49.eu', 'ACTIVE', '2024-01-01 00:00:00'),
(50, 'COMPANY', 'Kowalczyk Electronics',         'Warsaw',     '00-496', 'Poland',         'orders@company50.eu', 'ACTIVE', '2024-01-01 00:00:00');

-- CUSTOMERS — individuals
INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_TYPE, FIRST_NAME, LAST_NAME, CITY, POSTAL_CODE, COUNTRY, EMAIL, ACCOUNT_STATUS, CREATED_AT) VALUES
(2,  'INDIVIDUAL', 'Marie',    'Dupont',    'Paris',      '75008', 'France',      'marie.dupont@email.com',    'ACTIVE', '2024-01-01 00:00:00'),
(5,  'INDIVIDUAL', 'Hans',     'Meier',     'Zurich',     '8002',  'Switzerland', 'hans.meier@email.com',      'ACTIVE', '2024-01-01 00:00:00'),
(7,  'INDIVIDUAL', 'Wolfgang', 'Fischer',   'Vienna',     '1060',  'Austria',     'wolfgang.fischer@email.com','ACTIVE', '2024-01-01 00:00:00'),
(9,  'INDIVIDUAL', 'Lars',     'Pedersen',  'Copenhagen', '1620',  'Denmark',     'lars.pedersen@email.com',   'ACTIVE', '2024-01-01 00:00:00'),
(15, 'INDIVIDUAL', 'Erik',     'Andersson', 'Stockholm',  '11120', 'Sweden',      'erik.andersson@email.com',  'ACTIVE', '2024-01-01 00:00:00'),
(16, 'INDIVIDUAL', 'Aino',     'Virtanen',  'Helsinki',   '00100', 'Finland',     'aino.virtanen@email.com',   'ACTIVE', '2024-01-01 00:00:00'),
(20, 'INDIVIDUAL', 'Dimitar',  'Petrov',    'Sofia',      '1000',  'Bulgaria',    'dimitar.petrov@email.com',  'ACTIVE', '2024-01-01 00:00:00'),
(28, 'INDIVIDUAL', 'Nils',     'Eriksson',  'Gothenburg', '41101', 'Sweden',      'nils.eriksson@email.com',   'ACTIVE', '2024-01-01 00:00:00'),
(29, 'INDIVIDUAL', 'Matti',    'Koskinen',  'Tampere',    '33100', 'Finland',     'matti.koskinen@email.com',  'ACTIVE', '2024-01-01 00:00:00'),
(39, 'INDIVIDUAL', 'Anna',     'Svensson',  'Stockholm',  '11453', 'Sweden',      'anna.svensson@email.com',   'ACTIVE', '2024-01-01 00:00:00'),
(48, 'INDIVIDUAL', 'Olof',     'Bergström', 'Västerås',   '72212', 'Sweden',      'olof.bergstrom@email.com',  'ACTIVE', '2024-01-01 00:00:00');

-- SHIPPING_PRODUCTS (8 delivery service products)
INSERT INTO SHIPPING_PRODUCTS (PRODUCT_ID, NAME, DESCRIPTION, SERVICE_TYPE, MAX_WEIGHT_KG, MAX_LENGTH_CM, MAX_WIDTH_CM, MAX_HEIGHT_CM, ZONE, BASE_PRICE, PRICE_PER_KG, ESTIMATED_DAYS_MIN, ESTIMATED_DAYS_MAX, INSURANCE_INCLUDED, SIGNATURE_REQUIRED, ACTIVE) VALUES
(1, 'EU Standard Parcel',    'EU Standard Parcel shipping',    'STANDARD', 30,  120.0, 80.0, 60.0, 'EU',       9.90,  0.80, 3, 5, FALSE, FALSE, TRUE),
(2, 'EU Express Parcel',     'EU Express Parcel shipping',     'EXPRESS',  30,  120.0, 80.0, 60.0, 'EU',      24.90,  1.50, 1, 2, TRUE,  TRUE,  TRUE),
(3, 'EU Economy Pallet',     'EU Economy Pallet shipping',     'ECONOMY', 500,  120.0, 80.0, 60.0, 'EU',      49.90,  0.30, 5, 8, FALSE, FALSE, TRUE),
(4, 'EU Express Pallet',     'EU Express Pallet shipping',     'EXPRESS', 500,  120.0, 80.0, 60.0, 'EU',      89.90,  0.60, 1, 3, TRUE,  TRUE,  TRUE),
(5, 'Domestic Standard',     'Domestic Standard shipping',     'STANDARD', 30,  120.0, 80.0, 60.0, 'DOMESTIC', 5.90,  0.50, 2, 3, FALSE, FALSE, TRUE),
(6, 'Domestic Express',      'Domestic Express shipping',      'EXPRESS',  30,  120.0, 80.0, 60.0, 'DOMESTIC',14.90,  1.00, 1, 1, TRUE,  TRUE,  TRUE),
(7, 'EU Envelope',           'EU Envelope shipping',           'STANDARD',  2,  120.0, 80.0, 60.0, 'EU',       4.90,  0.00, 3, 5, FALSE, FALSE, TRUE),
(8, 'EU Express Envelope',   'EU Express Envelope shipping',   'EXPRESS',   2,  120.0, 80.0, 60.0, 'EU',      12.90,  0.00, 1, 2, TRUE,  TRUE,  TRUE);

-- =============================================================================
-- One complete end-to-end order example
-- Schneider Electronics (Berlin) -> Marie Dupont (Paris) via Frankfurt
-- =============================================================================

INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, ORIGIN_LOCATION_ID, PICKUP_ADDRESS, PICKUP_CITY, PICKUP_COUNTRY, DESTINATION_ADDRESS, DESTINATION_CITY, DESTINATION_COUNTRY, ESTIMATED_DELIVERY_DATE, TOTAL_AMOUNT, CURRENCY) VALUES
(1, 1, '2025-05-01 08:22:00', 'DELIVERED', 1, 'Alexanderplatz 5', 'Berlin', 'Germany', '42 Avenue des Champs', 'Paris', 'France', '2025-05-03', 28.15, 'EUR');

INSERT INTO ORDER_ITEMS (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DECLARED_WEIGHT_KG, DECLARED_CONTENTS, DECLARED_VALUE, INSURANCE_OPTED) VALUES
(1, 1, 2, 1, 24.90, 2.17, 'IoT sensor modules (qty 10)', 899.90, TRUE);

INSERT INTO PAYMENTS (PAYMENT_ID, ORDER_ID, PAYMENT_METHOD, CARD_LAST_FOUR, CARD_BRAND, CARD_COUNTRY, BILLING_ADDRESS, BILLING_CITY, BILLING_COUNTRY, PAYMENT_AMOUNT, CURRENCY, PAYMENT_STATUS, PAYMENT_TIMESTAMP, IP_ADDRESS, DEVICE_FINGERPRINT, TRANSACTION_REFERENCE) VALUES
(1, 1, 'CREDIT_CARD', '4821', 'VISA', 'Germany', 'Alexanderplatz 5', 'Berlin', 'Germany', 28.15, 'EUR', 'COMPLETED', '2025-05-01 08:23:00', '91.23.45.67', 'fp_sch001', 'TXN-2025-000001');

INSERT INTO PACKAGES (PACKAGE_ID, ORDER_ITEM_ID, ORDER_ID, TRACKING_NUMBER, ACTUAL_WEIGHT_KG, LENGTH_CM, WIDTH_CM, HEIGHT_CM, PACKAGE_TYPE, STATUS, CREATED_AT) VALUES
(1, 1, 1, 'TMS-2025-000001', 2.17, 30.0, 20.0, 12.0, 'BOX', 'DELIVERED', '2025-05-01 10:00:00');

INSERT INTO TRACKING_EVENTS (EVENT_ID, PACKAGE_ID, LOCATION_ID, EVENT_TIMESTAMP, EVENT_TYPE, STATUS, DESCRIPTION, CARRIER) VALUES
(1, 1, 1,    '2025-05-01 10:15:00', 'PICKED_UP',       'IN_TRANSIT', 'Picked up at Berlin Central Hub',  'DHL Express'),
(2, 1, 1,    '2025-05-01 18:00:00', 'DEPARTED_HUB',    'IN_TRANSIT', 'Departed Berlin Central Hub',       'DHL Express'),
(3, 1, 3,    '2025-05-01 22:30:00', 'ARRIVED_AT_HUB',  'IN_TRANSIT', 'Arrived Frankfurt Airport Hub',     'DHL Express'),
(4, 1, 3,    '2025-05-02 02:00:00', 'DEPARTED_HUB',    'IN_TRANSIT', 'On overnight flight to Paris',      'DHL Express'),
(5, 1, 5,    '2025-05-02 04:30:00', 'ARRIVED_AT_HUB',  'IN_TRANSIT', 'Arrived Paris North Hub',           'DHL Express'),
(6, 1, NULL, '2025-05-02 10:30:00', 'OUT_FOR_DELIVERY', 'IN_TRANSIT', 'With courier',                      'DHL Express'),
(7, 1, NULL, '2025-05-02 12:15:00', 'DELIVERED',        'DELIVERED',  'Signed by M. Dupont',               'DHL Express');

INSERT INTO DELIVERIES (DELIVERY_ID, PACKAGE_ID, DRIVER_NAME, VEHICLE_ID, SCHEDULED_DATE, ACTUAL_DELIVERY_DATE, RECIPIENT_NAME, SIGNATURE_COLLECTED, DELIVERY_NOTES, STATUS) VALUES
(1, 1, 'Jean-Luc Moreau', 'VAN-FR-088', '2025-05-02', '2025-05-02 12:15:00', 'Marie Dupont', TRUE, 'Signed at front desk', 'COMPLETED');
