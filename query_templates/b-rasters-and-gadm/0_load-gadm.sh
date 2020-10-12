shp2pgsql -I -s 4326 {{gadm_source_file}} {{db_schema}}.{{gadm_target_table}} | psql -h {{host_name}} -p 5432 -U {{user}} -d {{db_name}} -W
