CREATE OR REPLACE TASK transform.ga_ingest_task
    WAREHOUSE = XX_INGEST
    SCHEDULE = 'USING CRON 30 */12 * * * America/Los_Angeles'
    AS
    call db_google_analytics.transform.sp_ingest_google_analytics();
    
    
    ALTER TASK transform.ga_ingest_task RESUME;