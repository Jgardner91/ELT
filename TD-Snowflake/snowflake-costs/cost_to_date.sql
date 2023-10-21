
-- cost to date 
select account_name,round(sum(usage_in_currency), 2) as usage_in_currency
from snowflake.organization_usage.usage_in_currency_daily
where usage_date > DATEADD(Month,-1,CURRENT_TIMESTAMP())
group by 1
order by 2 desc;