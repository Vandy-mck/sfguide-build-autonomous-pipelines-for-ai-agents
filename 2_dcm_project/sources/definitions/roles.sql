define role SUMMIT_DEVELOPER_ROLE{{env_suffix}}
    comment = 'Openflow deployments/runtimes, dynamic tables, semantic views, cortex agents';

grant role SUMMIT_DEVELOPER_ROLE{{env_suffix}} to role {{project_owner_role}};

grant USAGE on database SUMMIT_DB{{env_suffix}} to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant USAGE on schema SUMMIT_DB{{env_suffix}}.OPENFLOW to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant USAGE on schema SUMMIT_DB{{env_suffix}}.RAW to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant USAGE on schema SUMMIT_DB{{env_suffix}}.TRANSFORM to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant USAGE on schema SUMMIT_DB{{env_suffix}}.ANALYTICS to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant USAGE on WAREHOUSE SUMMIT_WH{{env_suffix}} to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant SELECT on ALL dynamic tables in schema SUMMIT_DB{{env_suffix}}.TRANSFORM to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant SELECT, INSERT, UPDATE, TRUNCATE on ALL tables in schema SUMMIT_DB{{env_suffix}}.TRANSFORM to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};
grant SELECT on ALL views in schema SUMMIT_DB{{env_suffix}}.ANALYTICS to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};


define role SUMMIT_INGEST_ROLE{{env_suffix}}
    comment = 'Ingest data from Openflow into the raw zone';

grant role SUMMIT_INGEST_ROLE{{env_suffix}} to role SUMMIT_DEVELOPER_ROLE{{env_suffix}};

grant USAGE on database SUMMIT_DB{{env_suffix}} to role SUMMIT_INGEST_ROLE{{env_suffix}};
grant USAGE on schema SUMMIT_DB{{env_suffix}}.RAW to role SUMMIT_INGEST_ROLE{{env_suffix}};
grant USAGE on WAREHOUSE SUMMIT_WH{{env_suffix}} to role SUMMIT_INGEST_ROLE{{env_suffix}};
grant USAGE on INTEGRATION SUMMIT_EAI to role SUMMIT_INGEST_ROLE{{env_suffix}};
grant CREATE TABLE on schema SUMMIT_DB{{env_suffix}}.RAW to role SUMMIT_INGEST_ROLE{{env_suffix}};
grant SELECT, INSERT, UPDATE, DELETE, TRUNCATE on ALL tables in schema SUMMIT_DB{{env_suffix}}.RAW to role SUMMIT_INGEST_ROLE{{env_suffix}};
