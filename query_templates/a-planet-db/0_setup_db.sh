#!/bin/bash
psql -h {{host_address}} -p 5432 -U {{superuser}} -W -f create_db.sql
psql -h {{host_address}} -p 5432 -U {{superuser}} -d {{db_name}} -W -f setup_db.sql
psql -h {{host_address}} -p 5432 -U {{superuser}} -d {{db_name}} -W -f tbl_create_history.sql
psql -h {{host_address}} -p 5432 -U {{superuser}} -d {{db_name}} -W -f {{gbmi_schema}}_setup.sql
