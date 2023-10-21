CREATE OR REPLACE PROCEDURE transform.sp_ingest_sales_force()
    returns VARIANT
    language javascript
    EXECUTE AS CALLER
    as
    $$  
       
   
        //set enviornment 
        var db_set_env = snowflake.createStatement({sqlText : "USE db_sales_force"});
        db_set_env.execute();
        
        //define ingest related constants
        const SALES_FORCE_ENTITIES = [{name:"sales_force_pull"}];
        const STG_NAME = "stg_sales_force";
        const FF_NAME  = "sf_fmt";
        
        // truncate old data
        SALES_FORCE_ENTITIES.forEach(entity => 
        snowflake.createStatement({sqlText:`TRUNCATE TABLE raw.${entity.name}`}));
        
        // define copy statement
        SALES_FORCE_ENTITIES.forEach(entity => 
                                snowflake.createStatement({sqlText:`COPY INTO raw.${entity.name}
                                                           FROM @transform.${STG_NAME}
                                                           FILE_FORMAT = transform.${FF_NAME}`}).execute());
                                                           
        SALES_FORCE_ENTITIES.forEach(entity => 
                                snowflake.createStatement({sqlText:`INSERT OVERWRITE INTO content.${entity.name}
                                (deals_ds_id,
                                client,
                                current_status,
                                total_active_debt,
                                enrollment_date,
                                first_payment_date,
                                first_payment_cleared,
                                first_payment_cleared_date,
                                last_draft_status,
                                last_draft_date,
                                schedule_type,
                                cancel_inititation_date,
                                total_enrolled_debt_fpc,
                                total_drafts_cleared_count,
                                deals_owner,
                                true_debt,
                                true_fdb,
                                length_to_fdp)
                                SELECT 
                                deals_ds_id,
                                client,
                                current_status,
                                try_to_number(replace(replace(total_active_debt,'$',''),',','')) as total_active_debt,
                                enrollment_date,
                                first_payment_date,
                                first_payment_cleared,
                                first_payment_cleared_date,
                                last_draft_status,
                                last_draft_date,
                                schedule_type,
                                cancel_inititation_date,
                                try_to_number(replace(replace(total_enrolled_debt_fpc,'$',''),',','')) as total_enrolled_debt_fpc,
                                total_drafts_cleared_count,
                                deals_owner,
                                try_to_number(replace(replace(true_debt,'$',''),',','')) as true_debt,
                                true_fdb,
                                length_to_fdp
                                FROM raw.${entity.name}`}).execute());
                                                           
                                                           
        
      
$$;