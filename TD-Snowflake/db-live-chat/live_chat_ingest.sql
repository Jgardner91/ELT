create or replace procedure transform.live_chat_ingest()
    returns variant
    language javascript
    EXECUTE AS CALLER
    as
    $$  
       
   
        // set enviornment.
        var db_set_env = snowflake.createStatement({sqlText : "use db_live_chat"});
        db_set_env.execute();
        
        // defining truncate statements 
        var truncate_table = snowflake.createStatement({sqlText : "truncate table db_live_chat.raw.chat_transcripts"});
        
        
        // defining copy statements
        var load_chat_results = snowflake.createStatement({sqlText: "copy into raw.chat_transcripts from @transform.lv_stg_area/agent/"});
        var load_duration = snowflake.createStatement({sqlText: "copy into raw.duration from @transform.lv_stg_area/reports/duration"});
        var load_engagement = snowflake.createStatement({sqlText: "copy into raw.engagement from @transform.lv_stg_area/reports/engagement"});
        var load_form = snowflake.createStatement({sqlText: "copy into raw.form from @transform.lv_stg_area/reports/forms"});
        var load_performance = snowflake.createStatement({sqlText: "copy into raw.performance from @transform.lv_stg_area/reports/performance"});
        var load_ranking = snowflake.createStatement({sqlText: "copy into raw.ranking from @transform.lv_stg_area/reports/ranking"});
        var load_rating = snowflake.createStatement({sqlText: "copy into raw.rating from @transform.lv_stg_area/reports/ratings"});
  
        
        // executing copy statments
        load_chat_results.execute();
        load_duration.execute();
        load_engagement.execute();
        load_performance.execute();
        load_ranking.execute();
        load_form.execute();
        load_rating.execute();

  
        // moving chat transcirpts to staged schema
        var move_to_staged = snowflake.createStatement({sqlText: `INSERT OVERWRITE INTO staged.chat_transcripts(id,last_event_per_user,users,last_thread_summary,properties,access,is_followed)
                                                                  SELECT id,PARSE_JSON(last_event_per_user) as last_event_per_user,PARSE_JSON(users) as users, 
                                                                  PARSE_JSON(last_thread_summary) as last_thread_summary, PARSE_JSON(properties) as properties, PARSE_JSON(access) as access, is_followed
                                                        FROM raw.chat_transcripts`});
        // executing move to staged
        move_to_staged.execute();
  
        // executing truncate
        truncate_table.execute();
        
        //  logging 
        truncate_log = truncate_table.getNumRowsAffected();
        chats_log = load_chat_results.getNumRowsAffected();
        duration_log = load_duration.getNumRowsAffected();
        engagement_log = load_engagement.getNumRowsAffected();
        performance_log = load_performance.getNumRowsAffected();
        ranking_log =   load_ranking.getNumRowsAffected();
        form_log =      load_form.getNumRowsAffected();
        rating_log = load_rating.getNumRowsAffected();
        staging_log = move_to_staged.getNumRowsAffected();
        
       
        const log_ = {"chats_load":chats_log,"duration_load":duration_log,"engagement_load":engagement_log,"performance_load":performance_log,"ranking_load":ranking_log,
                       "form_load":form_log,"rating_load":rating_log,"staging":staging_log,"truncate":truncate_log};
        
        return log_;
    $$;