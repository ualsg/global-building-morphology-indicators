#!/bin/bash

{% if raster_name == "worldpop2020_1km" %}


wget {{raster_download_path}} -O {{raster_source_file}}
raster2pgsql -I -s 4326 -t "auto" {{ raster_source_file }} {{db_schema}}.{{raster_target_table}} | psql "host={{host_address}} port=5432 user={{user}} dbname={{db_name}} password='xxxxxxxx'"


{% else %}


wget {{raster_download_path}} -O {{raster_source_file}}
unzip {{raster_source_file}} "**.tif" -d unzipped_tifs && cd unzipped_tifs

i=0
for file in **/*.tif; do
  if [ -f "$file" ]; then
    if [[ "$i" -eq 0 ]]; then
       time raster2pgsql -I -s 4326 -t "auto" -p "$file" {{db_schema}}.{{raster_target_table}} | time psql "host={{host_address}} port=5432 user={{user}} dbname={{db_name}} password='xxxxxxxx'"
    fi
    time raster2pgsql -I -s 4326 -t "auto" -a "$file" {{db_schema}}.{{raster_target_table}} | time psql "host={{host_address}} port=5432 user={{user}} dbname={{db_name}} password='xxxxxxxx'"
    ((i+=1))
  fi
done


{% endif %}