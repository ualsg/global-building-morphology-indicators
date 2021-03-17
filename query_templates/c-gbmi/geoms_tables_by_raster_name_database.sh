#!/bin/bash


declare -a tbl_scripts=(\
  'buildings_by_{{raster_name}}' 'bga_by_{{raster_name}}' \
  'bgi_by_{{raster_name}}' 'bgi_clipped_by_{{raster_name}}')


host="{{ host_address }}"
database="{{ database }}"
gbmi_query_dir="{{ gbmi_query_dir }}"


for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "${database} ${script}"
  eval "$cmd"
done

