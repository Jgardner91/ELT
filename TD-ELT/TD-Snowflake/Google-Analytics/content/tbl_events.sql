CREATE OR REPLACE TABLE content.events(
   event_name                 varchar,
   event_count                number,
   total_users                number,
   event_count_per_user       number,
   total_revenue              number,
   data_warehouse_insert_time timestamp_ltz(9))