CREATE ROLE {{new_user}} WITH PASSWORD {{password}} LOGIN;

GRANT rds_superuser TO {{new_user}};  -- this is specific to AWS RDS
GRANT postgres TO {{new_user}};

-- easier to troubleshoot if needed
GRANT ALL PRIVILEGES ON DATABASE postgres TO {{new_user}};

-- power users
GRANT ALL PRIVILEGES ON DATABASE {{db_name}} TO {{new_user}};
GRANT ALL PRIVILEGES ON SCHEMA {{db_schema}} TO {{new_user}};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{db_schema}} TO {{new_user}};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{db_schema}} TO {{new_user}};
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{db_schema}} TO {{new_user}};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{db_schema}} TO {{new_user}};
