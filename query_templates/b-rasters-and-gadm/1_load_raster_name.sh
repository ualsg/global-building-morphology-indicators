raster2pgsql -I -s 4326 -t "auto" {{raster_source_file}} {{db_schema}}.{{raster_target_table}} | psql -h {{host_address}} -p 5432 -U {{user}} -d {{db_name}} -W
