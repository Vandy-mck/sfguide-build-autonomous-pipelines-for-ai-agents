-- =============================================================================
-- setup.sql — Pre-requisite roles and grants for DCM Project (Summit DE238)
--
-- Run with: snow sql -f 1_bootstrap/setup.sql -c <connection_name>
-- Requires: ACCOUNTADMIN role on the connection
-- =============================================================================

----------------------------------------------------------------------
-- 1. Create SUMMIT_ADMIN
--    Rights to create databases, schemas, and objects
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS SUMMIT_ADMIN;
SET user_name = (SELECT CURRENT_USER());
GRANT ROLE SUMMIT_ADMIN TO USER IDENTIFIER($user_name);
ALTER USER IDENTIFIER($user_name) SET DEFAULT_ROLE = SUMMIT_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE ROLE ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE EXTERNAL ACCESS INTEGRATION ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE OPENFLOW DEPLOYMENT ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SUMMIT_ADMIN;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE SUMMIT_ADMIN WITH GRANT OPTION;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE SUMMIT_ADMIN WITH GRANT OPTION;
GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_VIEWER TO ROLE SUMMIT_ADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SUMMIT_ADMIN;
SHOW GRANTS TO ROLE SUMMIT_ADMIN;

-- Allow all models
ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'ALL';
-- Allow cross-region inference across any supported region
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
----------------------------------------------------------------------
-- 2. Create the dcm project resources
--    This will create the database, schema and dcm project used later in the quickstart
----------------------------------------------------------------------
USE ROLE SUMMIT_ADMIN;
CREATE DATABASE IF NOT EXISTS DCM_DB;
CREATE SCHEMA IF NOT EXISTS DCM_DB.PROJECTS;
CREATE WAREHOUSE IF NOT EXISTS SUMMIT_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    COMMENT = 'For DCM Projects';
USE WAREHOUSE SUMMIT_WH;
ALTER USER IDENTIFIER($user_name) SET DEFAULT_WAREHOUSE = 'SUMMIT_WH';

CREATE DCM PROJECT IF NOT EXISTS DCM_DB.PROJECTS.DCM_PROJECT_DEV
    COMMENT = 'Quickstart DCM project for dev';

-- Create openflow deployment beforehand (it takes 8-10 minutes)
CREATE OPENFLOW DEPLOYMENT IF NOT EXISTS SUMMIT_DEPLOYMENT
 display_name = "Summit Deployment"
 deployment_type = "SNOWFLAKE";
DESCRIBE OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT;
SHOW GRANTS ON OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT;

-- Create external access integration to be shared with all participants
CREATE SCHEMA IF NOT EXISTS DCM_DB.NETWORK;

CREATE OR REPLACE NETWORK RULE DCM_DB.NETWORK.REDPANDA_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'seed-b1b12e51.d8c8o6pjqgbv1u6et180.fmc.prd.cloud.redpanda.com:9092',
        '0-b1b12e51-f076999.d8c8o6pjqgbv1u6et180.fmc.prd.cloud.redpanda.com:30092',
        '1-b1b12e51-c9ba39f.d8c8o6pjqgbv1u6et180.fmc.prd.cloud.redpanda.com:30092',
        '2-b1b12e51-d2f000a.d8c8o6pjqgbv1u6et180.fmc.prd.cloud.redpanda.com:30092'
    );
DESCRIBE NETWORK RULE DCM_DB.NETWORK.REDPANDA_NETWORK_RULE;

-- Create ONE external access integration with ALL network rules
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS SUMMIT_EAI
  ALLOWED_NETWORK_RULES = (
    DCM_DB.NETWORK.REDPANDA_NETWORK_RULE
  )
  ENABLED = TRUE
  COMMENT = 'Openflow SPCS runtime access integration';

DESCRIBE INTEGRATION SUMMIT_EAI;

----------------------------------------------------------------------
-- 3. Get your account identifier and username
--    (use these values to update manifest.yml)
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
       CURRENT_USER() AS user_name;

----------------------------------------------------------------------
-- 4. Tear down
--    Run this to remove all roles and revoke grants created above
----------------------------------------------------------------------
-- USE ROLE ACCOUNTADMIN;

-- ALTER OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT TERMINATE;
-- DROP DATABASE IF EXISTS DCM_DB;
-- DROP WAREHOUSE IF EXISTS SUMMIT_WH;
-- DROP ROLE IF EXISTS SUMMIT_ADMIN;
-- DROP INTEGRATION SUMMIT_EAI;
