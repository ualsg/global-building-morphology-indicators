#!/bin/bash

declare -a databases=({{ databases }})
declare -a raster_names=('worldpop2020_1km' 'worldpop2020_100m')

if [[ ${databases[-1]} == 'planet' ]]; then
  unset databases[-1]
fi

for db in "${databases[@]}"; do
  for raster_name in "${raster_names[@]}"; do
    if [[ "${db}" == "planet"  ||  "${db}" == "argentina"  ||  "${db}" == "new_zealand"  ||  "${db}" == "switzerland" ]] && [[ "${raster_name}" == "worldpop2020_100m" ]]; then
      echo "Skipping ${raster_name} for ${db}."
    else
      cmd1="bash ./geoms_tables_by_${raster_name}_${db}.sh"
      eval "${cmd1}"
    fi
  done
done
