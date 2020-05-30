# ETL with Postgres

This project creates a database of song and artist data and logs of user activity.  The project utilizes the star schema, with 4 fact tables: songs, artists, times, and users, and 1 dimension table: song plays.  3 Python modules, which utilize the Pandas and psycopg2 libraries, extract the data from log files, transform it, and load it into Postgres.

Duplicate inserts into the artists and songs are ignored.  Duplicate inserts into the user table results in the user's level being updated.

The create_tables module will create the Sparkify database and the 5 tables.  If the Sparkify database already exists, it will be dropped first and created new.

Use `python3 create_tables.py` to run.

The `sql_queries` module is used by both the `create_tables` and `etl` modules.

The `etl` module will scan the song and log data folders, create a list of files, and load them all into the Sparkify db.

Run with `python3 etl.py`