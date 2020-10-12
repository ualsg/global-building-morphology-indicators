## Global Building Morphology Indicators (GBMI)

### Sypnosis
This aim of this project is to generate a Global Building Morphology Indicators (GBMI) dataset based on the open data - OpenStreetMap. The python script exploit JinjaSQL and SQL templates to generate a collection of Bash scripts
  and SQL scripts that performs the database setup, data ingestion, data transformation and analysis that evetually
   yields a list of building indicators. 

### Steps 
Our original set up is on a PostgreSQL 12 database running on Amazon Web Services (AWS) RDS. PostGIS and a few other
 extensions are installed to support our spatial analysis. 
 
Before proceeding to running the following scripts, it is assumed the following pre-requisites are fulfilled:
    
- a bastion host is set up for connections to the RDS instance
- SSL connection is set up for the user(s) who need to connect to bastion host and to the RDS
- login credential of the master user on RDS instance

#### Section a: PostGIS and OpenStreetMap Database Setup

The bash scripts and queries under this section are to perform the following tasks:

- create a database to host the openstreetmap data and install PostGIS and other necessary
 extensions
- create new users, if specified in the configuration,  that allows connections and db
 operations on the
 openstreetmap
 database
- download openstreetmap
- load openstreetmap into the public schema in the new database (unless configured otherwise in
 the configuration)
- set up a `gbmi_schema` as configured in the configuration

#### Section b: Global Administrative Boundaries and Rasters

This section of the scripts are to:

- import rasters, as configured in the configurations. The current configurations supports more
 than one raster systems. One of the rasters we use, [WorldPop2020](https://www.worldpop.org), is
  publicly available [here](https://www.worldpop.org/geodata/summary?id=24777). 
- import the Global Administrative Boundary dataset into the database. We got ours from [here](https://gadm.org/download_world.html). 
- map the raster cells to GADM dataset so we could identify country, and administrative boundaries such as province, city.
- calculate aggregated areas of countries and administrative boundaries
 
 
#### Section c: Global Building Morphology Indices

This section are the queries that create the building morphology index tables.  


