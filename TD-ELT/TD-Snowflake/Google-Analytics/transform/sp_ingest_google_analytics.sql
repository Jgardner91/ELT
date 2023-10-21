CREATE OR REPLACE PROCEDURE transform.sp_ingest_google_analytics()
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
        var move_to_content = snowflake.createStatement({sqlText : 
          `INSERT OVERWRITE INTO content.events(event_name,
            event_count,
            total_users,
            event_count_per_user,
            total_revenue,
            data_warehouse_insert_time) 
          SELECT 
            try_to_number(EventName) as event_name, 
            try_to_number(EventCount) as event_count, 
            try_to_number(TotalUsers) as total_users, 
            try_to_number(EventCountPerUser) as event_count_per_user, 
            try_to_number(TotalRevenue) as total_revenue, 
            current_timestamp(2) as data_warehouse_insert_time 
        FROM raw.events`});
        move_to_content.execute();
        
        // grab results 
        truncate_result = truncate_table.getNumRowsAffected();
        copy_result = copy_into.getNumRowsAffected();
        move_to_content_result = move_to_content.getNumRowsAffected();
        
        
        results.push([truncate_result,copy_result,move_to_content]);
        
        return results;
    $$;