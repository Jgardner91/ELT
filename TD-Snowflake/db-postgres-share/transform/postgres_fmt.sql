USE db_postgres_share;
USE SCHEMA transform;

CREATE or REPLACE file format postgres_fmt
type = CSV
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"';