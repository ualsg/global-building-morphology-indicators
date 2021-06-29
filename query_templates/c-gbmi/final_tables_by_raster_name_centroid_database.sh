#!/bin/bash


declare -a tbl_scripts=(\
  'buildings_indicators_by_{{raster_name}}_centroid')
declare -a agg_levels=('cell' 'admin_div5' 'admin_div4' 'admin_div3' 'admin_div2' 'admin_div1' 'country')


host="{{ host_address }}"
database="{{ database }}"
gbmi_query_dir="{{ gbmi_query_dir }}"


for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "${database} ${script}"
  eval "$cmd"
done


for level in "${agg_levels[@]}"; do
  bgi_script="agg_bgi_by_${level}_{{raster_name}}"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${bgi_script}.sql"
  echo "${database} ${bgi_script}"
  eval "$cmd"
  bni_script="agg_bni_by_${level}_{{raster_name}}_centroid"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${bni_script}.sql"
  echo "${database} ${bni_script}"
  eval "$cmd"
done