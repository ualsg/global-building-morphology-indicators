#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
gbmi_query_dir="{{ gbmi_script_dir }}"


# Joint table of building geometric and neighbour indicators
declare -a tbl_scripts=(\
  'buildings_indicators_by_{{raster_name}}')

# the -w option prevents prompts for password, uses .pgpass
for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done


# Aggregated both geometric and neighbour building indicators at cell, admin divisions and country levels
# declare -a agg_levels=('cell' 'admin_div5' 'admin_div4' 'admin_div3' 'admin_div2' 'admin_div1' 'country')
declare -a agg_levels=({{ agg_levels }})
echo "$agg_levels"

# the -w option prevents prompts for password, uses .pgpass
for level in "${agg_levels[@]}"; do
  bgi_script="agg_bgi_by_${level}_{{raster_name}}"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${bgi_script}.sql"
  echo "$cmd"
  eval "$cmd"
  bni_script="agg_bni_by_${level}_{{raster_name}}"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${bni_script}.sql"
  echo "$cmd"
  eval "$cmd"
done