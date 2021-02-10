#!/bin/bash
shp2pgsql -I {{gadm_source_file}} {{db_schema}}.{{gadm_target_table}} | psql -h {{host_address}} -p 5432 -U {{user}} -d {{db_name}} -W
