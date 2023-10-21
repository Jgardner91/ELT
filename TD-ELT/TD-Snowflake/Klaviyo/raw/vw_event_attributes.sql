create or replace secure view published.vw_event_attributes as
  select distinct attributes:uuid::string as uuid ,attributes:"metric_id"::string as metric_id,attributes:profile_id::string as profile_id,
  attributes:event_properties:"$event_id"::string as event_id,attributes:event_properties:"$experiment"::string as experiment_id,
  attributes:event_properties:"Campaign Name"::string as campaign_name, attributes:event_properties:"Email Domain"::string as email_domain,
  attributes:event_properties:Subject::string as subject,
  attributes:datetime::string as date_time
  from content.events