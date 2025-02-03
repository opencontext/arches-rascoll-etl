# arches-rascoll-etl

This repo has some Python code used for "Extract Transform Load" (ETL) processes to move legacy reference and sample collection item data into an Arches instance. The Arches instance needs to have the "Arches for Reference and Sample Collection" (https://github.com/archesproject/arches-for-reference-and-sample-collection) application and package installed. For development purposes, one can deploy this "Arches for Reference and Sample Collection" ready instance using this Docker-based (https://github.com/opencontext/arches-via-docker/tree/local-rascoll-dev-2-x) approach.

## Background

We're using the Arches relational views feature (see documentation: https://arches.readthedocs.io/en/7.6/developing/reference/import-export/#sql-import) as the main route for the ETL. Python scripts take the legacy dataset and reshape these data into multiple "staging" tables stored in a "staging" schema on the Arches PostgreSQL database. Certain UUIDs are read or generated to make it easier to trace-back the provenance of data through the ETL process. The "staging" data tables are also saved externally as CSV files to facilitate debugging and replication. 

The Python scripts read configurations to understand how to transform raw data into data for the staging tables. These configurations are also used to generate SQL statements used to move data from the staging tables in the "staging" schema into various tables defined by the Arches relational views. These configurations and the code that reads these configurations should in theory be a useful basis for other Arches ETL needs beyond the "Arches for Reference and Sample Collection" application and package.

