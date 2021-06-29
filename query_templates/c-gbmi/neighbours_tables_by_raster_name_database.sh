#!/bin/bash


declare -a tbl_scripts=(\
  'bn_by_{{raster_name}}' \
  {% if not limit_buffer %}'bn_100_by_{{raster_name}}' {% endif %}'bn_50_by_{{raster_name}}' 'bn_25_by_{{raster_name}}' \
  'bni_by_{{raster_name}}')


host="{{ host_address }}"
database="{{ database }}"
gbmi_query_dir="{{ gbmi_query_dir }}"


for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "${database} ${script}"
  eval "$cmd"
done

