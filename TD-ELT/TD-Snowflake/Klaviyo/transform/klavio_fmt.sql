USE db_klavio;

CREATE OR REPLACE FILE FORMAT transform.klavio_fmt
  type = CSV
  field_delimiter = ','
  skip_header = 1
  field_optionally_enclosed_by = '"';