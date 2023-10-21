USE db_klavio;

CREATE OR REPLACE STAGE transform.stg_klavio
storage_integration = td_storage_integration
url = 's3://turbo-debt-dev/klavio'
file_format = transform.klavio_fmt