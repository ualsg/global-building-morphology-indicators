CREATE ROLE {{new_superuser}} WITH PASSWORD '{{password}}' LOGIN;

GRANT rds_superuser TO {{new_superuser}};  -- this is specific to AWS RDS
GRANT postgres TO {{new_superuser}};

-- easier to troubleshoot if needed
GRANT ALL PRIVILEGES ON DATABASE {{database}} TO {{new_superuser}};

--setup {{public_schema}} schema privileges
GRANT ALL PRIVILEGES ON SCHEMA {{public_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{public_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{public_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{public_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{public_schema}} TO {{new_superuser}};

--setup {{misc_schema}} schema privileges
GRANT ALL PRIVILEGES ON SCHEMA {{misc_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{misc_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{misc_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{misc_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{misc_schema}} TO {{new_superuser}};

--setup {{gbmi_schema}} schema privileges
GRANT ALL PRIVILEGES ON SCHEMA {{gbmi_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{gbmi_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{gbmi_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{gbmi_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{gbmi_schema}} TO {{new_superuser}};

--setup {{qa_schema}} schema privileges
GRANT ALL PRIVILEGES ON SCHEMA {{qa_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{qa_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{qa_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA {{qa_schema}} TO {{new_superuser}};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{qa_schema}} TO {{new_superuser}};

-- house cleaning
VACUUM ANALYZE;
