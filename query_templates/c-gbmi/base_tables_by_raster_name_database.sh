#!/bin/bash


declare -a tbl_scripts=(\
  'raster_polygons_{{raster_name}}' 'cells_{{raster_name}}')


host="{{ host_address }}"
database="{{ database }}"
public_query_dir="{{ public_query_dir }}"
gbmi_query_dir="{{ gbmi_query_dir }}"


for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${public_query_dir}/${script}.sql"
  echo "${database} ${script}"
  eval "$cmd"
done

