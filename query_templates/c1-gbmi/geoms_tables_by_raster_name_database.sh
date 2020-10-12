#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
gbmi_query_dir="{{ gbmi_script_dir }}"


# Compute Geometric attributes and indicators
declare -a tbl_scripts=(\
  'bga_by_{{raster_name}}' \
  'bgi_by_{{raster_name}}' 'bgi_clipped_by_{{raster_name}}')


# the -w option prevents prompts for password, uses .pgpass
for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done

