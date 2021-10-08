#!/bin/bash

declare -a databases=({{ databases }})
declare -a raster_names=('worldpop2020_1km' 'worldpop2020_100m')

if [[ ${databases[0]} == 'planet' ]]; then
  unset databases[0]
fi

for db in "${databases[@]}"; do
  for raster_name in "${raster_names[@]}"; do
    if [[ "${db}" == "planet"  ||  "${db}" == "argentina"  ||  "${db}" == "new_zealand"  ||  "${db}" == "switzerland" ]] && [[ "${raster_name}" == "worldpop2020_100m" ]]; then
      echo "Skipping ${raster_name} for ${db}."
    fi
    cmd1="bash ./final_tables_by_${raster_name}_${db}.sh"
    cmd2="bash ./final_tables_by_${raster_name}_centroid_${db}.sh"
    eval "${cmd1}"
    eval "${cmd2}"
  done
done
