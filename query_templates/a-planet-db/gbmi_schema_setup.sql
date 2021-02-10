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