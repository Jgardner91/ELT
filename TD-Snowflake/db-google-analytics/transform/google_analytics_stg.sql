use db_google_analytics;
use schema transform;

create or replace stage ga4_stg_area
storage_integration = td_storage_integration
url = 's3://turbo-debt-dev/google-analytics/'
file_format = transform.google_fmt

