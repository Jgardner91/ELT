CREATE OR REPLACE PROCEDURE transform.sp_postgres_full()
    returns variant 
    language javascript 
    EXECUTE AS CALLER 
    as 
    $$
        var results = [];
        
        // set enviornment 
        var db_set_env = snowflake.createStatement({sqlText : "use db_postgres_share"});
        db_set_env.execute();
    
        
        // defining truncate statements 
        var truncate_table = snowflake.createStatement({sqlText : "truncate table db_postgres_share.raw.first_leads"});
        truncate_table.execute();
        
        var copy_into = snowflake.createStatement({sqlText: "copy into raw.first_leads from @transform.stg_postgres/postgres-full ON_ERROR = 'CONTINUE'"});
        copy_into.execute();
   
   $$