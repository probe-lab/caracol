BEGIN;

create type query_type_new as enum
    (
        'prometheus',
        'elasticsearch_aggregate',
        'cloudwatch'
    );

ALTER TABLE queries
    ALTER COLUMN query_type TYPE query_type_new
        USING query_type::text::query_type_new;

DROP TYPE query_type;

ALTER TYPE query_type_new RENAME TO query_type;

COMMIT;