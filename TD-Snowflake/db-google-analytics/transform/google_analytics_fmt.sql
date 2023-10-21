  use db_google_analytics;
  use schema transform;
  create or replace file format google_fmt
  type = CSV
  field_delimiter = ','
  skip_header = 1
  field_optionally_enclosed_by = '"'