USE db_klavio;

create or replace table raw.lists(
  type VARCHAR,
  id VARCHAR,
  attributes VARCHAR,
  relationships VARCHAR,
  links VARCHAR);

create or replace table raw.segments(
  type VARCHAR,
  id VARCHAR,
  attributes VARCHAR,
  relationships VARCHAR,
  links VARCHAR);

create or replace table raw.events(
  type VARCHAR,
  id VARCHAR,
  attributes VARCHAR,
  relationships VARCHAR,
  links VARCHAR);

create or replace table raw.metrics(
  type VARCHAR,
  id VARCHAR,
  attributes VARCHAR,
  links VARCHAR);


