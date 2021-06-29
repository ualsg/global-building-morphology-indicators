## Global Building Morphology Indicators (GBMI)

### Sypnosis
This aim of this project is to generate a Global Building Morphology Indicators (GBMI) dataset based on the open data - OpenStreetMap. The python script exploit JinjaSQL and SQL templates to generate a collection of Bash scripts and SQL scripts that performs the database setup, data ingestion, data transformation and 
analysis that eventually yields a list of building indicators. 

The generation the GBMI datasets could be done globally or for any selected region/city. Each 
defined scope will be contained in its own database.  

The GBMI dataset generation is a process of 
running a series of bash scripts and SQL queries to generate more than 50 tables for one selected 
raster system alone. Many parts of the scripts are repetitive. For ease of maintenance, this 
repository offers a framework that generates 
these bash scripts and SQL queries using templates.
 

### Steps 
Our original set up is on a PostgreSQL 12 database running on Amazon Web Services (AWS) RDS. 
PostGIS and a few other  extensions are installed to support our spatial analysis. These 
extensions are: postgis, hstore, fuzzystrmatch, postgis_tiger_geocoder, postgis_topology.
 
Before proceeding to running the following scripts, it is assumed that there is a PostgreSQL 
database available, and there is a user created with enough credentials to run the scripts.

#### Section a: PostGIS and OpenStreetMap Database Setup

The bash scripts and queries under this section are to perform the following tasks:

- create a database (say `planet`) to host the openstreetmap data and install PostGIS and other 
  necessary
 extensions
- install these extensions: postgis, hstore, fuzzystrmatch, postgis_tiger_geocoder, postgis_topology   
- create new users, if specified in the configuration,  that allows connections and db
 operations on the openstreetmap database
- download openstreetmap planet 
- load openstreetmap into the public schema in the new database (unless configured otherwise in
 the configuration)
- set up a `misc` and `gbmi` schema as configured in the configuration

#### Section b: Global Administrative Boundaries and Rasters

We used the [World Population 2020](https://www.worldpop.org) rasters, specifically the dataset 
in these two different resolutions: 100m and 1km. Below are the process to get each of these 
rasters 
into the RDS respectively:

##### Worldpop 1KM Raster 

The world population dataset at at this resolution is already aggregated as one GeoTiff raster which can be donwloaded and processed in one go. 

Steps:
- download the Worldpop 1km raster from [here](http://ftp.worldpop.org/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif)
- import the rasters into the PostGIS database, via `raster2pgsql` command, based on the database 
  configurations in the config.json. 
  
##### Worldpop 100M Raster

The world population rasters at this resolution are only available by the individual countries. Therefore we loop through the GeoTiFFs and process them one by one. 

Steps:
- download the worldpop raster in 100m from [here](http://ftp.worldpop.org/2020/0_Mosaicked/global_mosaic_ppp_100m_2020_vrt.zip)
- unzip all the GeoTiff files into a directory 
- load the rasters into PostGIS database, via `raster2pgsql` command, based on the database configurations in the config.json file. 
  
##### GADM 

To map the rasters and osm data to country, province/state, city/town etc more comprehensively, 
we use the Global Database of Global Administrative Areas. The GADM data is available in GeoPKG and Shapefile format.

- download the global GADM dataset from here
- import the GADM dataset into the PostGIS database via `shp2pgsql` command, based on the database configurations in the config.json file.


Once the Worldpop rasters and GADM dataset are loaded, we then mapped the raster cells to GADM dataset so we could identify country, and administrative 
boundaries such as province, city. Aggregated areas of these countries, and administrative divisions are also subsequently calculated. 

 
 
#### Section c: Global Building Morphology Indicators

This section is the queries that create the building morphology indicator tables. The workflow and steps of computing the indicators are as follows:


##### Building Level Indicators: 

- `buildings`: extract top 97% of the buildings from `planet_osm_polygom` table based on 12 most frequent 'building' tag value
- `building_by_raster`: map buildings to each rasters we use in the system so we identify the country, province, state and city of the buildings
- `building_geom_attributes_by_raster`: extract the attributes that are needed for calculating the geometric building indicators from `building_by_raster` tables
- `building_geom_indicators_by_raster`: calculate the geometric building indicators from `buidling_geom_attributes_by_rasster` tables
- `building_neighbours_by_raster`: extract neighbours for each of the buildings within 100m, the maximum buffer of our study
- `building_neighbour_buffer_by_raster`: calculate the neighbour indicators for each building within 3 different buffers: 25, 50 and 100
- `building_neighbours_indicators_by_raster`: joint table of the `buildings_neighbour_buffer_by_raster` at 3 different buffers
- `buildings_indicators_by_raster`: joint table of `buildings_geom_indicators_by_raster` and `buildings_neighbour_indicators_by_raster`. 
  
##### Aggregated Building Indicators: 

The building indicators are aggregated at 5 GADM levels and at raster cell levels. Thus the aggregation levels are namely:

- raster cell
- country
- province or state (admin division 1)
- county or district (admin division 2)
- town or city (admin division 3)
- urban commune or municipals (admin division 4)

Two types of aggregated tables will be generated:
- `agg_buildings_geom_indicators_by_agg_level`: this is aggregation of the geometric building indicators from `buildings_geom_indicators_by_raster` tables
- `agg_buildings_indicators_agg_level`: these tables combined the aggregation of the geometric building indicators and building neighbour indicators from 
  `buildings_indicators_by_raster_name` tables
  
##### Miscellaneous tables: 

There are also a few tables that are helpful to inspect the quality and state of the OSM building 
data such as:
- `osm_polygon_attr_freqs`: this shows the frequency of values of building (osm_polygon) related 
  tags 
- `agg_buildings_height_levels_qa_by_agg_level_raster_name`: this shows aggregated statistics of 
  completeness of building height and building levels
  
#### Section d: Exporting the Global Building Morphology Indicators

With the configuration in `config.json` of the target scope/datasets to be exported, the python 
script also generates a list of bash scripts, one for each dataset, to export the GBMI indicators. 

The current export process supports ESRI Shapefiles `shp`, GeoPackage `gpkg`, Comma Separated 
Values `csv`, and GeoTiff `tiff`. 


For the city datasets, the exported files are organized under the following directory hierarchic 
structure:

-- {city}
    -- {aggregation-level}
        -- buildings_indicators_{raster_name}
        -- agg_bgi_by_{agg_level}_{raster_name}
        -- agg_bni_by_{agg_level}_{raster_name}

The export of various format are stored under the `{aggregation-level}` folder. 

For the global dataset, the export of aggregated data will be structured similar to city's. The 
building level data will be split to the highest division available for each country according 
to GADM 3.6. The files will be organized in tree structure. 

### Computed Global Building Morpological Index
|table column name|exported field name|description|
|---------------- | ----------------- | ----------|
|agg_geom_admin_div1|agg_geom|aggregated geometry of admin division 1|
|agg_geom_admin_div2|agg_geom|aggregated geometry of admin division 2|
|agg_geom_admin_div3|agg_geom|aggregated geometry of admin division 3|
|agg_geom_admin_div4|agg_geom|aggregated geometry of admin division 4|
|agg_geom_admin_div5|agg_geom|aggregated geometry of admin division 5|
|azimuth|az|the angular measurement of a building facing north or south|
|azimuth_cv|az_cv|the coefficient of variation, aka relative standard deviation (RSD) of azimuth|
|azimuth_d|az_d|the dispersion of azimuth|
|azimuth_max|az_max|the maximum of azimuth|
|azimuth_mean|az_mean|the mean of azimuth|
|azimuth_mean_pct_rnk|az_mpr|the mean percent rank of azimuth|
|azimuth_median|az_med|the median of azimuth|
|azimuth_min|az_min|the minimum of azimuth|
|azimuth_sd|az_sd|the standard deviation of azimuth|
|buffer_area_100|ba100|the buffer area 100m from a building footprint|
|buffer_area_100_mean|ba100_mean|the mean of 100m buffer areas of buildings|
|buffer_area_25|ba25|the buffer area 25m from a building footprint|
|buffer_area_25_mean|ba25_mean|the mean of 25m buffer areas of buildings|
|buffer_area_50|ba50|the buffer area 50m from a building footprint|
|buffer_area_50_mean|ba50_mean|the mean of 50m buffer areas of buildings|
|building:levels|blvl|number of levels in a building|
|building:levels_cv|blvl_cv|the coefficient of variation, aka relative standard deviation (RSD) of building levels|
|building:levels_d|blvl_d|the dispersion of building levels|
|building:levels_max|blvl_max|the maximum of building levels|
|building:levels_mean|blvl_mean|the mean of building levels|
|building:levels_mean_pct_rnk|blvl_mpr|the percent rank of mean of building levels|
|building:levels_median|blvl_med|the median of building levels|
|building:levels_min|blvl_min|the minimum of building levels|
|building:levels_pct_rnk|blvl_pr|the precent rank of building levels|
|building:levels_sd|blvl_sd|the standard deviation of building levels|
|buildings_count|bc|the count of buildings|
|buildings_count_normalised|bc_nm|normalized count of building by the aggregated area|
|cell_admin_div1|c_adm_div1|admin division 1 of cell based on GADM 3.6|
|cell_admin_div2|c_adm_div2|admin division 2 of cell based on GADM 3.6|
|cell_admin_div3|c_adm_div3|admin division 3 of cell based on GADM 3.6|
|cell_admin_div4|c_adm_div4|admin division 4 of cell based on GADM 3.6|
|cell_admin_div5|c_adm_div5|admin division 5 of cell based on GADM 3.6|
|cell_area|c_area|area of cell|
|cell_centroid|c_centroid|centroid of cell|
|cell_country|c_country|country of cell|
|cell_country_code2|c_cc2|2 character country code of cell|
|cell_country_code3|c_cc3|3 character country code of cell|
|cell_country_official_name|c_con|official country name of cell|
|cell_geom|c_geom|geometry of cell|
|cell_id|c_id|id of cell|
|cell_population|c_pop|estimated popuplation of the cell (based on worldpop2020)|
|compactness|cpct|the compactness of a building |
|compactness_cv|cpct_cv|the coefficient of variation, aka relative standard deviation (RSD) of compactness|
|compactness_d|cpct_d|the dispersion of compactness|
|compactness_max|cpct_max|the maximum of compactness|
|compactness_mean|cpct_mean|the mean of compactness|
|compactness_mean_pct_rnk|cpct_mpr|the percent rank of mean compactness|
|compactness_median|cpct_med|the median of compactness|
|compactness_min|cpct_min|the minimum of compactness|
|compactness_pct_rnk|cpct_pr|the percent rank of compactness|
|compactness_sd|cpct_sd|the standard deviation of compactness|
|complexity|cplx|the complexity of a building|
|complexity_cv|cplx_cv|the coefficient of variation, aka relative standard deviation (RSD) of complexity|
|complexity_d|cplx_d|the dispersion of complexity|
|complexity_max|cplx_max|the maximum of complexity|
|complexity_mean|cplx_mean|the mean of complexity|
|complexity_mean_pct_rnk|cplx_mpr|the percent rank of mean complexity|
|complexity_median|cplx_med|the median of complexity|
|complexity_min|cplx_min|the minimum of complexity|
|complexity_pct_rnk|cplx_pr|the percent rank of complexity|
|complexity_sd|cplx_sd|the standard deviation of complexity|
|distance_100_cv|d100_cv|the coefficient of variation, aka relative standard deviation (RSD) of distances of neighbours within 100m|
|distance_100_d|d100_d|the dispersion of distances of neighbours within 100m|
|distance_100_max|d100_max|the maximum of distances of neighbours within 100m|
|distance_100_mean|d100_mean|the mean of distances of neighbours within 100m|
|distance_100_mean_pct_rnk|d100_mpr|the percent rank of mean distances of neighbours within 100m|
|distance_100_median|d100_med|the median of distances of neighbours within 100m|
|distance_100_min|d100_min|the minimum of distances of neighbours within 100m|
|distance_100_sd|d100_sd|the standard deviation of distances of neighbours within 100m|
|distance_25_cv|d25_cv|the coefficient of variation, aka relative standard deviation (RSD) of distances of neighbours within 25m|
|distance_25_d|d25_d|the dispersion of distances of neighbours within 25m|
|distance_25_max|d25_max|the maximum of distances of neighbours within 25m|
|distance_25_mean|d25_mean|the mean of distances of neighbours within 25m|
|distance_25_mean_pct_rnk|d25_mpr|the percent rank of mean distances of neighbours within 25m|
|distance_25_median|d25_med|the median of distances of neighbours within 25m|
|distance_25_min|d25_min|the minimum of distances of neighbours within 25m|
|distance_25_sd|d25_sd|the standard deviation of distances of neighbours within 25m|
|distance_50_cv|d50_cv|the coefficient of variation, aka relative standard deviation (RSD) of distances of neighbours within 50m|
|distance_50_d|d50_d|the dispersion of distances of neighbours within 50m|
|distance_50_max|d50_max|the maximum of distances of neighbours within 50m|
|distance_50_mean|d50_mean|the mean of distances of neighbours within 50m|
|distance_50_mean_pct_rnk|d50_mpr|the percent rank of mean distances of neighbours within 50m|
|distance_50_median|d50_med|the median of distances of neighbours within 50m|
|distance_50_min|d50_min|the minimum of distances of neighbours within 50m|
|distance_50_sd|d50_sd|the standard deviation of distances of neighbours within 50m|
|envelope_area|ea|the envelope area of a building|
|envelope_area_cv|ea_cv|the coefficient of variation, aka relative standard deviation (RSD) of envelope area|
|envelope_area_d|ea_d|the dispersion of envelope area|
|envelope_area_max|ea_max|the maximum of envelope area|
|envelope_area_mean|ea_mean|the mean of envelope area|
|envelope_area_mean_pct_rnk|ea_mpr|the percent rank of mean envelope area|
|envelope_area_median|ea_med|the median of envelope area|
|envelope_area_min|ea_min|the minimum of envelope area|
|envelope_area_pct_rnk|ea_pr|the percent rank of envelope area|
|envelope_area_sd|ea_sd|the standard deviation of envelope area|
|envelope_area_sum|ea_sm|the sum of envelope area|
|envelope_area_sum_normalised|ea_s_nm|the normalized sum of envelope area by the aggregated area|
|equivalent_rectangular_index|eri|the equivalent rectangular index of a building|
|equivalent_rectangular_index_cv|eri_cv|the coefficient of variation, aka relative standard deviation (RSD) of equivalent rectangular index|
|equivalent_rectangular_index_d|eri_d|the dispersion of equivalent rectangular index|
|equivalent_rectangular_index_max|eri_max|the maximum of equivalent rectangular index|
|equivalent_rectangular_index_mean|eri_mean|the mean of equivalent rectangular index|
|equivalent_rectangular_index_mean_pct_rnk|eri_mpr|the percent rank of mean equivalent rectangular index|
|equivalent_rectangular_index_median|eri_med|the median of equivalent rectangular index|
|equivalent_rectangular_index_min|eri_min|the minimum of equivalent rectangular index|
|equivalent_rectangular_index_pct_rnk|eri_pr|the percent rank of equivalent rectangular index|
|equivalent_rectangular_index_sd|eri_sd|the standard deviation of equivalent rectangular index|
|floor_area|fla|the floor area of a building|
|floor_area_cv|fla_cv|the coefficient of variation, aka relative standard deviation (RSD) of floor area|
|floor_area_d|fla_d|the dispersion of floor area|
|floor_area_max|fla_max|the maximum of floor area|
|floor_area_mean|fla_mea|the mean of floor area|
|floor_area_mean_pct_rnk|fla_mpr|the percent rank of mean floor area|
|floor_area_median|fla_med|the median of floor area|
|floor_area_min|fla_min|the minimum of floor area|
|floor_area_pct_rnk|fla_pr|the percent rank of floor area|
|floor_area_sd|fla_sd|the standard deviation of floor area|
|floor_area_sum|fla_sm|the sum of floor area|
|floor_area_sum_normalised|fla_s_nm|the normalized sum of floor area by the aggregated area|
|footprint_area|fpa|the footprint area of a building|
|footprint_area_cv|fpa_cv|the coefficient of variation, aka relative standard deviation (RSD) of footprint area|
|footprint_area_d|fpa_d|the dispersion of footprint area|
|footprint_area_max|fpa_max|the maximum of footprint area|
|footprint_area_mean|fpa_mean|the mean of footprint area|
|footprint_area_mean_pct_rnk|fpa_mpr|the percent rank of mean footprint area|
|footprint_area_median|fpa_med|the median of footprint area|
|footprint_area_min|fpa_min|the minimum of footprint area|
|footprint_area_pct_rnk|fpa_pr|the percent rank of footprint area|
|footprint_area_sd|fpa_sd|the standard deviation of footprint area|
|footprint_area_sum|fpas|the sum of footprint area|
|footprint_area_sum_normalised|fpas_nm|the normalized sum of footprint area by the aggregated area|
|height|h|the height of a building in meter|
|height_cv|h_cv|the coefficient of variation, aka relative standard deviation (RSD) of height|
|height_d|h_d|the dispersion of height|
|height_max|h_max|the maximum of height|
|height_mean|h_mean|the mean of height|
|height_mean_pct_rnk|h_mpr|the percent rank of mean height|
|height_median|h_med|the median of height|
|height_min|h_min|the minimum of height|
|height_pct_rnk|h_pr|the percent rank of height|
|height_sd|h_sd|the standard deviation of height|
|height_weighted_mean|h_wmean|the weighted mean of height by building footprint area|
|is_residential|res|whether a building is residential |
|mbr|mbr|the minimum bounding rectangular (mbr) of a building|
|mbr_area|mbra|the area of the mbr of a building|
|mbr_area_cv|mbra_cv|the coefficient of variation, aka relative standard deviation (RSD) of mbr area|
|mbr_area_d|mbra_d|the dispersion of mbr area|
|mbr_area_max|mbra_max|the maximum of mbr area|
|mbr_area_mean|mbra_mean|the mean of mbr area|
|mbr_area_mean_pct_rnk|mbra_mpr|the percent rank of mean mbr area|
|mbr_area_median|mbra_med|the median of mbr area|
|mbr_area_min|mbra_min|the minimum of mbr area|
|mbr_area_pct_rnk|mbra_pr|the percent rank of mbr area|
|mbr_area_sd|mbra_sd|the standard deviation of mbr area|
|mbr_area_sum|mbra_sm|the sum of mbr area|
|mbr_area_sum_normalized|mbra_s_nm|the normalized sum of mbr area by the aggregated area|
|mbr_length|l|the longest edge of the minimum bounding rectangular of a building|
|mbr_length_cv|l_cv|the coefficient of variation, aka relative standard deviation (RSD) of mbr length|
|mbr_length_d|l_d|the dispersion of mbr length|
|mbr_length_max|l_max|the maximum of mbr length|
|mbr_length_mean|l_mean|the mean of mbr length|
|mbr_length_mean_pct_rnk|l_mpr|the percent rank of mean mbr length|
|mbr_length_median|l_med|the median of mbr length|
|mbr_length_min|l_min|the minimum of mbr length|
|mbr_length_pct_rnk|l_pr|the percent rank of mbr length|
|mbr_length_sd|l_sd|the standard deviation of mbr length|
|mbr_width|w|the shortest edge of the minimum bounding rectangular of a building|
|mbr_width_cv|w_cv|the coefficient of variation, aka relative standard deviation (RSD) of mbr width|
|mbr_width_d|w_d|the dispersion of mbr width|
|mbr_width_max|w_max|the maximum of mbr width|
|mbr_width_mean|w_mean|the mean of mbr width|
|mbr_width_mean_pct_rnk|w_mpr|the percent rank of mean mbr width|
|mbr_width_median|w_med|the median of mbr width|
|mbr_width_min|w_min|the minimum of mbr width|
|mbr_width_pct_rnk|w_pr|the percent rank of mbr width|
|mbr_width_sd|w_sd|the standard deviation of mbr width|
|neighbour_100_count|n100c|the count of neighbour buildings within 100m|
|neighbour_100_count_cv|n100c_cv|the coefficient of variation, aka relative standard deviation (RSD) of neighbour count within 100m|
|neighbour_100_count_d|n100c_d|the dispersion of neighbour count within 100m|
|neighbour_100_count_max|n100c_max|the maximum of neighbour count within 100m|
|neighbour_100_count_mean|n100c_mean|the mean of neighbour count within 100m|
|neighbour_100_count_mean_pct_rnk|n100c_mpr|the percent rank of mean neighbour count within 100m|
|neighbour_100_count_median|n100c_med|the median of neighbour count within 100m|
|neighbour_100_count_min|n100c_min|the minimum of neighbour count within 100m|
|neighbour_100_count_sd|n100c_sd|the standard deviation of neighbour count within 100m|
|neighbour_25_count|n25c|the count of neighbour buildings within 25m|
|neighbour_25_count_cv|n25c_cv|the coefficient of variation, aka relative standard deviation (RSD) of neighbour count within 25m|
|neighbour_25_count_d|n25c_d|the dispersion of neighbour count within 25m|
|neighbour_25_count_max|n25c_max|the maximum of neighbour count within 25m|
|neighbour_25_count_mean|n25c_mean|the mean of neighbour count within 25m|
|neighbour_25_count_mean_pct_rnk|n25c_mpr|the percent rank of mean neighbour count within 25m|
|neighbour_25_count_median|n25c_med|the median of neighbour count within 25m|
|neighbour_25_count_min|n25c_min|the minimum of neighbour count within 25m|
|neighbour_25_count_sd|n25c_sd|the standard deviation of neighbour count within 25m|
|neighbour_50_count|n50c|the count of neighbour buildings within 50m|
|neighbour_50_count_cv|n50c_cv|the coefficient of variation, aka relative standard deviation (RSD) of neighbour count within 50m|
|neighbour_50_count_d|n50c_d|the dispersion of neighbour count within 50m|
|neighbour_50_count_max|n50c_max|the maximum of neighbour count within 50m|
|neighbour_50_count_mean|n50c_mean|the mean of neighbour count within 50m|
|neighbour_50_count_mean_pct_rnk|n50c_mpr|the percent rank of mean neighbour count within 50m|
|neighbour_50_count_median|n50c_med|the median of neighbour count within 50m|
|neighbour_50_count_min|n50c_min|the minimum of neighbour count within 50m|
|neighbour_50_count_sd|n50c_sd|the standard deviation of neighbour count within 50m|
|neighbour_footprint_area_100_cv|fpa100_cv|the coefficient of variation, aka relative standard deviation (RSD) of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_d|fpa100_d|the dispersion of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_max|fpa100_max|the maximum of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_mean|fpa100_mea|the mean of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_mean_pct_rnk|fpa100_mpr|the percent rank of mean footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_median|fpa100_med|the median of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_min|fpa100_min|the minimum of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_sd|fpa100_sd|the standard deviation of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_sum|fpa100_sum|the sum of footprint areas of neighbours within 100m|
|neighbour_footprint_area_100_sum_cv|fpa100scv|the coefficient of variation, aka relative standard deviation (RSD) of the footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_d|fpa100sd|the dispersion of the footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_max|fpa100smax|the maximum of footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_mean|fpa100smean|the mean of footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_mean_pct_rnk|fpa100smpr|the percent rank of mean footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_median|fpa100smed|the median of footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_min|fpa100smin|the minimum of footprint area sum of neighbours within 100m|
|neighbour_footprint_area_100_sum_sd|fpa100ssd|the standard deviation of footprint area sum of neighbours within 100m|
|neighbour_footprint_area_25_cv|fpa25_cv|the coefficient of variation, aka relative standard deviation (RSD) of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_d|fpa25_d|the dispersion of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_max|fpa25_max|the maximum of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_mean|fpa25_mean|the mean of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_mean_pct_rnk|fpa25_mpr|the percent rank of mean footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_median|fpa25_med|the median of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_min|fpa25_min|the minimum of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_sd|fpa25_sd|the standard deviation of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_sum|fpa25_sum|the sum of footprint areas of neighbours within 25m|
|neighbour_footprint_area_25_sum_cv|fpa25scv|the coefficient of variation, aka relative standard deviation (RSD) of the footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_d|fpa25sd|the dispersion of the footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_max|fpa25smax|the maximum of footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_mean|fpa25smean|the mean of footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_mean_pct_rnk|fpa25smpr|the percent rank of mean footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_median|fpa25smed|the median of footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_min|fpa25smin|the minimum of footprint area sum of neighbours within 25m|
|neighbour_footprint_area_25_sum_sd|fpa25ssd|the standard deviation of footprint area sum of neighbours within 25m|
|neighbour_footprint_area_50_cv|fpa50_cv|the coefficient of variation, aka relative standard deviation (RSD) of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_d|fpa50_d|the dispersion of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_max|fpa50_max|the maximum of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_mean|fpa50_mean|the mean of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_mean_pct_rnk|fpa50_mpr|the percent rank of mean footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_median|fpa50_med|the median of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_min|fpa50_min|the minimum of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_sd|fpa50_sd|the standard deviation of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_sum|fpa50_sum|the sum of footprint areas of neighbours within 50m|
|neighbour_footprint_area_50_sum_cv|fpa50scv|the coefficient of variation, aka relative standard deviation (RSD) of the footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_d|fpa50sd|the dispersion of the footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_max|fpa50smax|the maximum of footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_mean|fpa50smean|the mean of footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_mean_pct_rnk|fpa50smpr|the percent rank of mean footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_median|fpa50smed|the median of footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_min|fpa50smin|the minimum of footprint area sum of neighbours within 50m|
|neighbour_footprint_area_50_sum_sd|fpa50ssd|the standard deviation of footprint area sum of neighbours within 50m|
|osm_id|osm_id|OpenStreetMap id|
|perimeter|p|the perimeter of a building footprint|
|perimeter_cv|p_cv|the coefficient of variation, aka relative standard deviation (RSD) of perimeter|
|perimeter_d|p_d|the dispersion of perimeter|
|perimeter_max|p_max|the maximum of perimeter|
|perimeter_mean|p_mean|the mean of perimeter|
|perimeter_mean_pct_rnk|p_mpr|the percent rank of mean perimeter|
|perimeter_median|p_med|the median of perimeter|
|perimeter_min|p_min|the minimum of perimeter|
|perimeter_pct_rnk|p_pr|the percent rank of perimeter|
|perimeter_sd|p_sd|the standard deviation of perimeter|
|perimeter_sum|p_sm|the sum of perimeter|
|perimeter_sum_normalised|p_s_nm|the normalized sum of perimeter by the aggregated area|
|ratio_height_to_footprint_area|rhfpa|the ratio of height to footprint area of a building|
|ratio_height_to_footprint_area_cv|rhfpa_cv|the coefficient of variation, aka relative standard deviation (RSD) of height to footprint area ratio|
|ratio_height_to_footprint_area_d|rhfpa_d|the dispersion of height to footprint area ratio|
|ratio_height_to_footprint_area_max|rhfpa_max|the maximum of height to footprint area ratio|
|ratio_height_to_footprint_area_mean|rhfpa_mean|the mean of height to footprint area ratio|
|ratio_height_to_footprint_area_mean_pct_rnk|rhfpa_mpr|the percent rank of mean height to footprint area ratio|
|ratio_height_to_footprint_area_median|rhfpa_med|the median of height to footprint area ratio|
|ratio_height_to_footprint_area_min|rhfpa_min|the minimum of height to footprint area ratio|
|ratio_height_to_footprint_area_pct_rnk|rhfpa_pr|the percent rank of height to footprint area ratio|
|ratio_height_to_footprint_area_sd|rhfpa_sd|the standard deviation of height to footprint area ratio|
|ratio_neighbour_footprint_sum_to_buffer_100|rfb100|the ratio of the sum of neighbour footprint areas to the buffer area 100m from a building footprint|
|ratio_neighbour_footprint_sum_to_buffer_100_cv|rfb100_cv|the coefficient of variation of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_d|rfb100_d|the dispersion of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_max|rfb100_max|the max of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_mean|rfb100_mea|the mean of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_mean_pct_rnk|rfb100_mpr|the percent rank of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_median|rfb100_med|the median of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_min|rfb100_min|the minimum of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_100_sd|rfb100_sd|the standard deviation of the neighbour footprint sum to the buffer area ratio within 100m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25|rfb25|the ratio of the sum of neighbour footprint areas to the buffer area 25m from a building footprint|
|ratio_neighbour_footprint_sum_to_buffer_25_cv|rfb25_cv|the coefficient of variation of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_d|rfb25_d|the dispersion of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_max|rfb25_max|the max of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_mean|rfb25_mea|the mean of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_mean_pct_rnk|rfb25_mpr|the percent rank of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_median|rfb25_med|the median of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_min|rfb25_min|the minimum of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_25_sd|rfb25_sd|the standard deviation of the neighbour footprint sum to the buffer area ratio within 25m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50|rfb50|the ratio of the sum of neighbour footprint areas to the buffer area 50m from a building footprint|
|ratio_neighbour_footprint_sum_to_buffer_50_cv|rfb50_cv|the coefficient of variation of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_d|rfb50_d|the dispersion of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_max|rfb50_max|the max of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_mean|rfb50_mea|the mean of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_mean_pct_rnk|rfb50_mpr|the percent rank of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_median|rfb50_med|the median of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_min|rfb50_min|the minimum of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_footprint_sum_to_buffer_50_sd|rfb50_sd|the standard deviation of the neighbour footprint sum to the buffer area ratio within 50m from a building|
|ratio_neighbour_height_to_distance_100_cv|rhd100_cv|the coefficient of variation, aka relative standard deviation (RSD) of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_d|rhd100_d|the dispersion of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_max|rhd100_max|the maximum of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_mean|rhd100_mea|the mean of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_mean_pct_rnk|rhd100_mpr|the percent rank of mean height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_median|rhd100_med|the median of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_min|rhd100_min|the minimum of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_100_sd|rhd100_sd|the standard deviation of height to distance ratios of neighbours within 100m|
|ratio_neighbour_height_to_distance_25_cv|rhd25_cv|the coefficient of variation, aka relative standard deviation (RSD) of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_d|rhd25_d|the dispersion of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_max|rhd25_max|the maximum of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_mean|rhd25_mean|the mean of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_mean_pct_rnk|rhd25_mpr|the percent rank of mean height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_median|rhd25_med|the median of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_min|rhd25_min|the minimum of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_25_sd|rhd25_sd|the standard deviation of height to distance ratios of neighbours within 25m|
|ratio_neighbour_height_to_distance_50_cv|rhd50_cv|the coefficient of variation, aka relative standard deviation (RSD) of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_d|rhd50_d|the dispersion of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_max|rhd50_max|the maximum of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_mean|rhd50_mean|the mean of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_mean_pct_rnk|rhd50_mpr|the percent rank of mean height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_median|rhd50_med|the median of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_min|rhd50_min|the minimum of height to distance ratios of neighbours within 50m|
|ratio_neighbour_height_to_distance_50_sd|rhd50_sd|the standard deviation of height to distance ratios of neighbours within 50m|
|residential_count|resc|the count of residential buildings|
|residential_count_normalised|resc_nm|the normalized count of residential buildings by aggregated area|
|residential_floor_area_cv|rfla_cv|the coefficient of variation, aka relative standard deviation (RSD) of residential floor area|
|residential_floor_area_d|rfla_d|the dispersion of residential floor area|
|residential_floor_area_max|rfla_max|the maximum of residential floor area|
|residential_floor_area_mean|rfla_mean|the mean of residential floor area|
|residential_floor_area_mean_pct_rnk|rfla_mpr|the percent rank of mean residential floor area|
|residential_floor_area_median|rfla_med|the median of residential floor area|
|residential_floor_area_min|rfla_min|the minimum of residential floor area|
|residential_floor_area_sd|rfla_sd|the standard deviation of residential floor area|
|residential_floor_area_sum|rfla_sm|the sum of residential floor area|
|residential_floor_area_sum_normalised|rfla_s_nm|the normalized sum of residential floor area by the aggregated area|
|start_date|stdt|the open date or completion date of a building|
|start_date_pct_rnk|stdt_pr|the percent rank of completion date of a building|
|tags|tags|key value pair attributes|
|vertices_count|vc|the count of vertices of a building|
|vertices_count_cv|vc_cv|the coefficient of variation, aka relative standard deviation (RSD) of vertice count|
|vertices_count_d|vc_d|the dispersion of vertice count|
|vertices_count_max|vc_max|the maximum of vertice count|
|vertices_count_mean|vc_mean|the mean of vertice count|
|vertices_count_mean_pct_rnk|vc_mpr|the percent rank of mean vertice count|
|vertices_count_median|vc_med|the median of vertice count|
|vertices_count_min|vc_min|the minimum of vertice count|
|vertices_count_pct_rnk|vc_pr|the percent rank of vertice count|
|vertices_count_sd|vc_sd|the standard deviation of vertice count|
|vertices_count_sum|vc_sm|the sum of vertice count|
|vertices_count_sum_normalised|vc_s_nm|the normalized sum of vertice count by the aggregated area|
|volume|vol|the volume of a building|
|volume_cv|vol_cv|the coefficient of variation, aka relative standard deviation (RSD) of volume|
|volume_d|vol_d|the dispersion of volume|
|volume_max|vol_max|the maximum of volume|
|volume_mean|vol_mean|the mean of volume|
|volume_mean_pct_rnk|vol_mpr|the percent rank of mean volume|
|volume_median|vol_med|the median of volume|
|volume_min|vol_min|the minimum of volume|
|volume_pct_rnk|vol_pr|the percent rank of volume|
|volume_sd|vol_sd|the standard deviation of volume|
|volume_sum|vol_sm|the sum of volume|
|volume_sum_normalised|vol_s_nm|the normalized sum of volume by the aggregated area|
|wall_area|wa|the wall area of a building|
|wall_area_cv|wa_cv|the coefficient of variation, aka relative standard deviation (RSD) of wall area|
|wall_area_d|wa_d|the dispersion of wall area|
|wall_area_max|wa_max|the maximum of wall area|
|wall_area_mean|wa_mean|the mean of wall area|
|wall_area_mean_pct_rnk|wa_mpr|the percent rank of mean wall area|
|wall_area_median|wa_med|the median of wall area|
|wall_area_min|wa_min|the minimum of wall area|
|wall_area_pct_rnk|wa_pr|the percent rank of wall area|
|wall_area_sd|wa_sd|the standard deviation of wall area|
|wall_area_sum|wa_sm|the sum of wall area|
|wall_area_sum_normalised|wa_s_nm|the normalized sum of wall area by the aggregated area|
|way|way|shape geometry of a building|
|way_centroid|way_c|centroid geometry of a building|



### How to customize these sql queries for your need

One simply need to checkout the repo, install the python packages based on the `requirements.txt`, set up configurations on `config.json` and run `python main.py`.  All the queries and scripts will be generated to an `query_output` folder. 

