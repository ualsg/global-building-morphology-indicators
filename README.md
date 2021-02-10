## Global Building Morphology Indicators (GBMI)

### Sypnosis
This aim of this project is to generate a Global Building Morphology Indicators (GBMI) dataset based on the open data - OpenStreetMap. The python script exploit JinjaSQL and SQL templates to generate a collection of Bash scripts and SQL scripts that performs the database setup, data ingestion, data transformation and 
analysis that eventually yields a list of building indicators. 

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


The building indicators are aggregated at 5 GADM levels and at raster levels. Thus the aggregation levels are namely:

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
  
