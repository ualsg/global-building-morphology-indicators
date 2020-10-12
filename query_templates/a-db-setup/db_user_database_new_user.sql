CREATE ROLE {{new_user}} WITH PASSWORD '{{password}}' LOGIN;

-- easier to troubleshoot if needed
GRANT CONNECT ON DATABASE {{database}} TO {{new_user}};

--setup public schema privileges
GRANT CREATE, USAGE ON SCHEMA public TO {{new_user}};
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO {{new_user}};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {{new_user}};
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO {{new_user}};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {{new_user}};

-- setup misc schema privileges
GRANT CREATE, USAGE ON SCHEMA misc TO {{new_user}};
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA misc TO {{new_user}};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA misc TO {{new_user}};
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA misc TO {{new_user}};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA misc TO {{new_user}};

-- setup gbmi schema privileges
GRANT CREATE, USAGE ON SCHEMA gbmi TO {{new_user}};
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA gbmi TO {{new_user}};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gbmi TO {{new_user}};
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA gbmi TO {{new_user}};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA gbmi TO {{new_user}};

-- setup osm_qa schema privileges
GRANT CREATE, USAGE ON SCHEMA osm_qa TO {{new_user}};
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA osm_qa TO {{new_user}};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA osm_qa TO {{new_user}};
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA osm_qa TO {{new_user}};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA osm_qa TO {{new_user}};

-- house cleaning
VACUUM ANALYZE;
