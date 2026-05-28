-- =============================================================================
-- Tear-down script for 2_dcm_project
-- Drops all resources created by the DCM project and post-deploy script.
-- Order: Openflow runtime -> database -> warehouse -> roles -> DCM project
--
-- snow sql -f scripts/tear_down.sql --variable "env_suffix=_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE SUMMIT_ADMIN;

-- 1. Suspend the Openflow runtime and wait for it to stop
ALTER OPENFLOW RUNTIME SUMMIT_DB{{env_suffix}}.OPENFLOW.SUMMIT_RUNTIME SUSPEND;
CALL SYSTEM$WAIT(60, 'SECONDS');
DROP OPENFLOW RUNTIME IF EXISTS SUMMIT_DB{{env_suffix}}.OPENFLOW.SUMMIT_RUNTIME;

-- 2. Drop the database (removes all schemas, tables, dynamic tables, and data)
DROP DATABASE IF EXISTS SUMMIT_DB{{env_suffix}};

-- 3. Drop the warehouse
DROP WAREHOUSE IF EXISTS SUMMIT_WH{{env_suffix}};

-- 4. Drop the roles (children first)
DROP ROLE IF EXISTS SUMMIT_INGEST_ROLE{{env_suffix}};
DROP ROLE IF EXISTS SUMMIT_DEVELOPER_ROLE{{env_suffix}};

-- 5. Drop the DCM project object
DROP DCM PROJECT IF EXISTS DCM_DB.PROJECTS.DCM_PROJECT{{env_suffix}};
