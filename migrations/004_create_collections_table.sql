create table collections
(
  query_id   integer not null,
  seq        integer not null,
  value      float not null,

  -- The query_id should reference the queries table.
  constraint fk_collections_query_id foreign key (query_id) references queries (id) on delete cascade,

  primary key (query_id,seq)

);
---- create above / drop below ----

drop table if exists collections;
