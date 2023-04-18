create type interval_type as enum
(
    'hourly',
    'daily',
    'weekly'
);

create type query_type as enum
(
    'prometheus',
    'elasticsearch_aggregate'
);

create table queries
(
  id         integer primary key generated always as identity,
  name       varchar not null,
  source_id  integer not null,
  query      varchar not null,
  query_type query_type not null,
  interval   interval_type not null,
  start      timestamptz  not null,
  finish     timestamptz,

  -- The source_id should reference the sources table.
  constraint fk_queries_source_id foreign key (source_id) references sources (id) on delete cascade

);
---- create above / drop below ----

drop table if exists queries;

drop type if exists query_type;

drop type if exists interval_type;
