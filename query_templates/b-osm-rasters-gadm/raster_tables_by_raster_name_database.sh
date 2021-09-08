#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
query_dir="{{ public_script_dir }}"
gbmi_source_dir="{{ gbmi_source_dir }}"
raster_name="{{ raster_name }}"
raster_file_suffix="{{ raster_file_suffix }}"


# Load rasters (worldpop 1km or 100m)
# the -w option prevents prompts for password, uses .pgpass
raster_cmd="time raster2pgsql -I -s 4326 -t \"auto\" ${gbmi_source_dir}/${database}/${database}${raster_file_suffix}.tif public.raw_tiles_${raster_name} | psql -h ${host} -p 5432 -d ${database} -w"
echo "$raster_cmd"
eval "$raster_cmd"


# Vectorize loaded rasters
declare -a tbl_scripts=(\
  'raster_polygons_{{raster_name}}' \
  'cells_{{raster_name}}')

# the -w option prevents prompts for password, uses .pgpass
for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done
