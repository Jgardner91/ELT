CREATE OR REPLACE PROCEDURE transform.sp_ingest_klaviyo()
    returns variant
    language javascript
    EXECUTE AS CALLER
    as
    $$  
       
   
        // set enviornment 
        var db_set_env = snowflake.createStatement({sqlText : "USE db_klaviyo"});
        db_set_env.execute();
        
        // define ingest related constants
        const KLAVIO_ENTITIES = [{name:"segments",sourcePattern:"segments"},{name:"events",sourcePattern:"events"},{name:"metrics",sourcePattern:"metrics"},{name:"lists",sourcePattern:"lists"}];
        const STG_NAME = "stg_klavio";
        const FF_NAME = "klavio_fmt";
        
        // truncate old data
        KLAVIO_ENTITIES.forEach(entity => 
        snowflake.createStatement({sqlText:`TRUNCATE TABLE raw.${entity.name}`}));
        
        // define copy statement
        KLAVIO_ENTITIES.forEach(entity => 
                                snowflake.createStatement({sqlText:`COPY INTO raw.${entity.name}
                                                           FROM @transform.${STG_NAME}/${entity.sourcePattern}
                                                           FILE_FORMAT = transform.${FF_NAME}`}).execute());
                                                
       
   $$;



   