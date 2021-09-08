#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
misc_query_dir="{{ misc_script_dir }}"
gbmi_query_dir="{{ gbmi_script_dir }}"
raster_name="{{raster_name}}"

# Determine most frequent `building` tag values, so we select the buildings from top most frequent
# the -w option prevents prompts for password, uses .pgpass
cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${misc_query_dir}/osm_polygon_attr_freqs.sql"
echo "$cmd"
eval "$cmd"


# Extract buildings, map buildings to raster cells
declare -a tbl_scripts=(\
  'buildings' 'buildings_by_{{raster_name}}')

# the -w option prevents prompts for password, uses .pgpass
for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${gbmi_query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done


# Aggregated building height and levels
declare -a agg_levels=({{ agg_levels }})
echo "$agg_levels"

# the -w option prevents prompts for password, uses .pgpass
for level in "${agg_levels[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${misc_query_dir}/agg_buildings_height_levels_qa_by_${level}_${raster_name}.sql"
  echo "$cmd"
  eval "$cmd"
done