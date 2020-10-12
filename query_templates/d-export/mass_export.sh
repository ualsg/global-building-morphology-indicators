#!/bin/bash

declare -a databases=({{ databases }})
declare -a raster_names=('worldpop2020_1km' 'worldpop2020_100m')

if [[ ${databases[0]} == 'planet' ]]; then
  unset databases[0]
fi

for db in "${databases[@]}"; do
  for raster_name in "${raster_names[@]}"; do
    if [[  "${db}" == "planet"  ||  "${db}" == "argentina"  ||  "${db}" == "new_zealand"  ||  "${db}" == "switzerland" ]] && [[ "${raster_name}" == "worldpop2020_100m" ]]; then
      echo "Skipping ${raster_name} export for ${db}."
    fi
    cmd="bash ./export_${raster_name}_${db}.sh"
#    echo "${cmd}"
    eval "${cmd}"
  done
done

for db in "${databases[@]}"; do
  for raster_name in "${raster_names[@]}"; do
    if [[ "${db}" == "argentina"  ||  "${db}" == "new_zealand"  ||  "${db}" == "switzerland" ]] && [[ "${raster_name}" == "worldpop2020_100m" ]]; then
      echo "Skipping ${raster_name} GeoTiFF export for ${db}."
    fi
    cmd="bash ./export_tiff_${raster_name}_${db}.sh"
#    echo "${cmd}"
    eval "${cmd}"
  done
done
