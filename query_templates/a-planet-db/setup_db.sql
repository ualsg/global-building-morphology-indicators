-- create extensions
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
CREATE EXTENSION postgis_topology;


-- this is somehow required for PGSQL12 and POSTGIS 3+
CREATE EXTENSION postgis_raster;


-- change owner of schemas
ALTER SCHEMA tiger OWNER TO rds_superuser;
ALTER SCHEMA tiger_data OWNER TO rds_superuser;
ALTER SCHEMA topology OWNER TO rds_superuser;


ALTER TABLE {{db_schema}}.spatial_ref_sys
    OWNER TO rds_superuser;
GRANT SELECT, INSERT ON TABLE {{db_schema}}.spatial_ref_sys TO rds_superuser;

-- setup public schema
CREATE SCHEMA IF NOT EXISTS {{db_schema}} AUTHORIZATION rds_superuser;
GRANT ALL PRIVILEGES ON SCHEMA {{db_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{db_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{db_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{db_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{db_schema}} TO rds_superuser;

-- house cleaning
VACUUM ANALYZE;
