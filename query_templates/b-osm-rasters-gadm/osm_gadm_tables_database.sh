#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
public_schema="{{public_schema}}"
script_dir="{{ public_script_dir }}"
site_source_dir="{{ site_source_dir }}"
osm_source_file="{{ osm_source_file }}"
gadm_source_dir="{{ gadm_source_dir }}"
gadm_source_file="{{gadm_source_file}}"
gadm_target_table="{{gadm_target_table}}"


# Load OSM PBF file
# Without '-W' option, this doesn't prompt for password, might use .pgpass
osm_cmd="time osm2pgsql --create --slim --cache 10000 --keep-coastlines --flat-nodes /data/tmp-osm --hstore --extra-attributes -l --host ${host} --port 5432 --database ${database} ${site_source_dir}/${database}/${osm_source_file}"
echo "$osm_cmd"
eval "$osm_cmd"

# create indices for the osm tables, the -w option prevents prompts for password, uses .pgpass
indices_cmd="psql -h ${host} -p 5432 -d ${database} -w -f ${script_dir}/osm_indices_${database}.sql"
echo "$indices_cmd"
eval "$indices_cmd"

# Load GADM36 shape, that contains administrative boundaries
# without '-W', this doesn't prompt for password, might use .pgpass
gadm_cmd="time shp2pgsql -I -s 4326 ${gadm_source_dir}/${gadm_source_file} ${public_schema}.${gadm_target_table} | psql -h ${host} -p 5432 -d ${database} -w"
echo "$gadm_cmd"
eval "$gadm_cmd"


# Run Scripts to load country_codes table, generate agg geom/area tables
declare -a tbl_scripts=(\
  'country_codes' 'agg_gadm36_areas' \
  'agg_gadm36_geom_areas_admin_div5' \
  'agg_gadm36_geom_areas_admin_div4' \
  'agg_gadm36_geom_areas_admin_div3' \
  'agg_gadm36_geom_areas_admin_div2' \
  'agg_gadm36_geom_areas_admin_div1' \
  'agg_gadm36_geom_areas_country')

# the -w option prevents prompts for password, uses .pgpass
for script in "${tbl_scripts[@]}"; do
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${script_dir}/${script}.sql"
  eval "$cmd"
done
