create or replace procedure transform.ga_ingest()
    returns variant
    language javascript
    EXECUTE AS CALLER
    as
    $$  
       
        // defining results array
        var results = [];
        
        // set enviornment 
        var db_set_env = snowflake.createStatement({sqlText : "use db_google_analytics"});
        db_set_env.execute();
        
        // defining truncate statements 
        var truncate_table = snowflake.createStatement({sqlText : "truncate table db_google_analytics.raw.events"});
        truncate_table.execute();
        
        // defining copy statements
        var copy_into = snowflake.createStatement({sqlText: "copy into raw.events from @transform.ga4_stg_area"})
        copy_into.execute();
        
        
        // move to content 
        var move_to_content = snowflake.createStatement({sqlText : "INSERT OVERWRITE INTO content.events (EventName,EventCount,TotalUsers,EventCountPerUser,TotalRevenue,DataWarehouseInsertTime) SELECT EventName, EventCount, TotalUsers, EventCountPerUser, TotalRevenue, current_timestamp(2) as DataWarehouseInsertTime FROM raw.events"});
        move_to_content.execute();
        
        // grab results 
        truncate_result = truncate_table.getNumRowsAffected();
        copy_result = copy_into.getNumRowsAffected();
        move_to_content_result = move_to_content.getNumRowsAffected();
        
        
        results.push([truncate_result,copy_result,move_to_content]);
        
        return results;
    $$;