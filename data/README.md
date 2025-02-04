# data

This directory stores both the legacy data (after cleanup in Open Refine, see: `gci-all-orig.csv`) and derived datasets prepared to add to the staging schema of the Arches/RASColl PostreSQL database for eventual ETL.

Because I never managed to get certain SQL statements to execute via SQLAlchemy, the SQL statements used to migrate data from the staging schema in the Arches/RASColl PostreSQL database to the relational views are also saved in the `data` directory.
