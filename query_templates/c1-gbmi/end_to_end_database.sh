#!/bin/bash


database="{{ database }}"
declare -a raster_names=('worldpop2020_1km' 'worldpop2020_100m')


declare -a commands=(\
  "bash ./db_setup_${database}.sh" \
  "bash ./osm_gadm_tables_${database}.sh")
for cmd in "${commands[@]}"; do
  echo "\n ${cmd}"
  eval "$cmd"
done

for raster_name in "${raster_names[@]}"; do
  # Compute neighbour table, neighbour indicators within buffer
  declare -a commands=(\
    "bash ./raster_tables_by_${raster_name}_${database}.sh" \
    "bash ./base_tables_by_${raster_name}_${database}.sh" \`
    "bash ./geoms_tables_by_${raster_name}_${database}.sh" \
    "bash ./neighbours_tables_by_${raster_name}_${database}.sh" \
    "bash ./neighbours_tables_by_${raster_name}_centroid_${database}.sh" \
    "bash ./final_tables_by_${raster_name}_${database}.sh" \
    "bash ./final_tables_by_${raster_name}_centroid_${database}.sh" \
    "bash ./export_${raster_name}_${database}.sh" \
    "bash ./export_tiff_${raster_name}_${database}.sh")

  if [[ "${database}" == "planet"  ||  "${database}" == "argentina"  ||  "${database}" == "new_zealand"  ||  "${database}" == "switzerland" ]] && [[ "${raster_name}" == "worldpop2020_100m" ]]; then
    echo "Skipping ${raster_name} for ${database}."
  else
    for cmd in "${commands[@]}"; do
      echo "$cmd"
      eval "$cmd"
    done
  fi
done
