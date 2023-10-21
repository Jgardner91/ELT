create or replace table staged.chat_transcripts (

id varchar, 
last_event_per_user variant,
users variant,
last_thread_summary variant,
properties variant,
access variant, 
is_followed varchar
)