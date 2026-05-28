-- =============================================================================
-- ANALYTICS LAYER: Views on top of TRANSFORM dynamic tables
-- These are the consumer-facing objects for dashboards, agents, and analysts.
-- =============================================================================

define view SUMMIT_DB{{env_suffix}}.ANALYTICS.LOCATION_ACTIVITY
as
    SELECT * FROM SUMMIT_DB{{env_suffix}}.TRANSFORM.DT_LOCATION_ACTIVITY;

define view SUMMIT_DB{{env_suffix}}.ANALYTICS.ORDER_SUMMARY
as
    SELECT * FROM SUMMIT_DB{{env_suffix}}.TRANSFORM.DT_ORDER_SUMMARY;

define view SUMMIT_DB{{env_suffix}}.ANALYTICS.PACKAGE_TRACKING
as
    SELECT * FROM SUMMIT_DB{{env_suffix}}.TRANSFORM.DT_PACKAGE_TRACKING;

define view SUMMIT_DB{{env_suffix}}.ANALYTICS.PACKAGE_HOPS
as
    SELECT * FROM SUMMIT_DB{{env_suffix}}.TRANSFORM.DT_PACKAGE_HOPS;

define view SUMMIT_DB{{env_suffix}}.ANALYTICS.FRAUD_DETECTION
as
    SELECT * FROM SUMMIT_DB{{env_suffix}}.TRANSFORM.DT_FRAUD_DETECTION;
