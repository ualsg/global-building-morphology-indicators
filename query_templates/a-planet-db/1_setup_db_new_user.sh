#!/bin/bash
psql -h {{host_address}} -p 5432 -U {{superuser}} -d {{db_name}} -W -f db_user_{{new_user}}.sql