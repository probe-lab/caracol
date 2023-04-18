
create type api_type as enum
(
    'grafanacloud',
    'elasticsearch'
);

create type auth_type as enum
(
    'bearer_token',
    'basic_auth'
);

create table providers
(
  id         integer primary key generated always as identity,
  name       varchar not null,
  api_type   api_type not null,
  api_url    varchar not null,
  auth_type  auth_type not null,

  constraint uq_providers_name UNIQUE (name)

);

---- create above / drop below ----

drop table if exists providers;

drop type if exists auth_type;

drop type if exists api_type;
