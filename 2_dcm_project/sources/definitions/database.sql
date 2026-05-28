define database SUMMIT_DB{{env_suffix}}
    comment = 'Summit DE238 database';

define schema SUMMIT_DB{{env_suffix}}.OPENFLOW
    comment = 'Schema for Openflow ingestion';

define schema SUMMIT_DB{{env_suffix}}.RAW
    comment = 'Raw landing zone for ingested data';

define schema SUMMIT_DB{{env_suffix}}.TRANSFORM
    comment = 'Transformation layer with dynamic tables';

define schema SUMMIT_DB{{env_suffix}}.ANALYTICS
    comment = 'Analytics layer with semantic views and agents';

define warehouse SUMMIT_WH{{env_suffix}}
with
    warehouse_size = '{{wh_size}}'
    auto_suspend = 300
    comment = 'Summit DE238 warehouse';
