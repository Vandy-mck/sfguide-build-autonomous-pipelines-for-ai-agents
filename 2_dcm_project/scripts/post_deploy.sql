-- =============================================================================
-- Post-deploy script for 2_dcm_project
-- Run after DCM DEPLOY using SUMMIT_ADMIN role
-- 
-- snow sql -f scripts/post_deploy.sql --variable "env_suffix=_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE SUMMIT_ADMIN;

GRANT USAGE ON OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT TO ROLE SUMMIT_DEVELOPER_ROLE{{env_suffix}};

CREATE OPENFLOW RUNTIME IF NOT EXISTS SUMMIT_DB{{env_suffix}}.OPENFLOW.SUMMIT_RUNTIME
    IN OPENFLOW DEPLOYMENT SUMMIT_DEPLOYMENT
    DISPLAY_NAME = "Summit Runtime"
    MIN_NODES = 1
    MAX_NODES = 1
    NODE_TYPE = 'SMALL'
    EXECUTE_AS_ROLE = 'SUMMIT_INGEST_ROLE{{env_suffix}}'
    EXTERNAL_ACCESS_INTEGRATIONS = (SUMMIT_EAI);

GRANT OPERATE, USAGE, MONITOR ON OPENFLOW RUNTIME SUMMIT_DB{{env_suffix}}.OPENFLOW.SUMMIT_RUNTIME TO ROLE SUMMIT_DEVELOPER_ROLE{{env_suffix}};
GRANT USAGE ON OPENFLOW RUNTIME SUMMIT_DB{{env_suffix}}.OPENFLOW.SUMMIT_RUNTIME TO ROLE SUMMIT_INGEST_ROLE{{env_suffix}};
