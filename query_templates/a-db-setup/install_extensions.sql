-- create extensions
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
CREATE EXTENSION postgis_topology;

-- this is for monitoring performance

-- this is for auditing and monitoring performance
CREATE EXTENSION pgaudit;
SET pgaudit.log = 'read,write,ddl';
SET pgaudit.log_catalog = 'off';
SET pgaudit.log_client = 'on';
SET pgaudit.log_relation = 'on';
SET pgaudit.log_parameter = 'on';

CREATE EXTENSION pg_stat_statements;

-- this is somehow required for PGSQL12 and POSTGIS 3+
CREATE EXTENSION postgis_raster;


-- change owner of schemas
ALTER SCHEMA tiger OWNER TO rds_superuser;
ALTER SCHEMA tiger_data OWNER TO rds_superuser;
ALTER SCHEMA topology OWNER TO rds_superuser;


ALTER TABLE {{public_schema}}.spatial_ref_sys
    OWNER TO rds_superuser;
GRANT SELECT, INSERT ON TABLE {{public_schema}}.spatial_ref_sys TO rds_superuser;
