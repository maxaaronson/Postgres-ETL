# Redshift Data Warehouse

This project copies log and song data from S3 buckets to Redshift.  This is done by first copying the data to Redshift staging tables, and then into analytics tables with a star schema.

### Getting Started

- The project can be run from whatever folder the Python scripts are located in

### Requirements

- ```dwh.cfg``` must exist and be accessible to the Python scripts.  This file must contain the login info for the cluster.  It must also have the ARN for an IAM Role with read access to the S3 buckets.

- configparser and psycopg2 Python modules must be installed and accessible to the Python interpreter

### Python Scripts

- ```sql_queries.py```

Contains the queries for ```drop```, ```create```, ```copy``` and ```insert``` commands.  These are passed to the other Python files.

- ```create_tables.py```

Connects to the Redshift cluster via the ```psycopg2``` module and creates the required tables.  Existing tables are dropped if they exist.  

- ```etl.py```

Connects to the Redshift cluster via the ```psycopg2``` module and first copies the required data from S3 buckets to the Redshift staging tables.  Then performs inserts via SQL queries to populate the analytics tables.

### Testing

The cluster can be queried to verify successful table creation and loads/inserts.  Redshift creates the tables under a "public" schema.