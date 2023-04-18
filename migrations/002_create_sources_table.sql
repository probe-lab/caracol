create table sources
(
  id              integer primary key generated always as identity,
  name            varchar not null,
  provider_id     integer not null,
  dataset         varchar,

  -- Only one provider_id/dataset combination should be allowed.
  constraint uq_sources_provider_id_dataset UNIQUE (provider_id, dataset),

  -- The provider_id should reference the providers table.
  constraint fk_sources_provider_id foreign key (provider_id) references providers (id) on delete cascade

);

---- create above / drop below ----

drop table if exists sources;
