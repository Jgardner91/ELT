-- table creation statements

create or replace table raw.duration (

    Day date,
    AvgAgentChatDuration number,
    TotalChats varchar,
    Duration number)
    
create or replace table raw.performance (
    Agent varchar,
    TimeAcceptingChats number,
    TotalChats number,
    BadChats number,
    GoodChats number,
    ChatTime number,
    FirstResponse number,
    AvgFirstReplyTime number,
    TimeLoggidIn number,
    TimeNotAcceptingChats number
)


create or replace table raw.engagement (
  
  Day date,
  ChatsStartedCustomer number,
  ChatsStartedAgent number
)


create or replace table raw.form(
Form_id varchar,
Form_count varchar,
groups varchar,
Field_id varchar,
Yes number, 
No number)



create or replace table raw.ranking(
Agent varchar,
 BadChats number,
  GoodChats number,
  AgentScore numeric,
  TotalRatedChats number
)


create or replace table raw.rating(
    Day date,
     NumBadChats number,
     Chats number, 
     NumGoodChats number
)


