CREATE OR REPLACE TASK transform.postgres_ingest_task
    WAREHOUSE = XX_INGEST
    SCHEDULE = 'USING CRON 30 */12 * * * UTC'
    AS
    call db_postgres_share.transform.postgres_ingest()