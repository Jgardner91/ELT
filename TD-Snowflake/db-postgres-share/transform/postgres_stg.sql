use db_postgres_share;
use schema transform;

create or replace stage postgres_stg_area
storage_integration = td_storage_integration
url = 's3://turbo-debt-dev/postgres-sql/postgres-delta/'