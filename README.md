## Global Building Morphology Indicators (GBMI)

### Sypnosis
This aim of this project is to generate a Global Building Morphology Indicators (GBMI) dataset based on the open sourced
 OpenStreetMap data. The python script exploit JinjaSQL and SQL templates to generate a collection of Bash scripts
  and SQL scripts that performs the database setup, data ingestion, data transformation and analysis that evetually
   yields a list of building indicators. 

### Steps 
Our original set up is on a PostgreSQL 12 database running on Amazon Web Services (AWS) RDS. PostGIS and a few other
 extensions are installed to support our spatial analysis. 
 
Before proceeding to running the following scripts, it is assumed the following pre-requisites are fulfilled:
    
- a bastion host is set up for connections to the RDS instance
- SSL connection is set up for the user(s) who need to connect to bastion host and to the RDS
- login credential of the master user on RDS instance

