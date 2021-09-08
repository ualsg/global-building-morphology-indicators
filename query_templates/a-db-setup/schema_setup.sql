
-- setup public schema
CREATE SCHEMA IF NOT EXISTS {{public_schema}} AUTHORIZATION rds_superuser;
GRANT ALL PRIVILEGES ON SCHEMA {{public_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{public_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{public_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{public_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{public_schema}} TO rds_superuser;


-- setup misc schema
CREATE SCHEMA IF NOT EXISTS {{misc_schema}} AUTHORIZATION rds_superuser;
GRANT ALL PRIVILEGES ON SCHEMA {{misc_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{misc_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{misc_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{misc_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{misc_schema}} TO rds_superuser;


-- setup gbmi schema
CREATE SCHEMA IF NOT EXISTS {{gbmi_schema}} AUTHORIZATION rds_superuser;
GRANT ALL PRIVILEGES ON SCHEMA {{gbmi_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{gbmi_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{gbmi_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{gbmi_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{gbmi_schema}} TO rds_superuser;

-- setup osm_qa schema
CREATE SCHEMA IF NOT EXISTS {{qa_schema}} AUTHORIZATION rds_superuser;
GRANT ALL PRIVILEGES ON SCHEMA {{qa_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{qa_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{qa_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{qa_schema}} TO rds_superuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{qa_schema}} TO rds_superuser;

-- house cleaning
VACUUM ANALYZE;