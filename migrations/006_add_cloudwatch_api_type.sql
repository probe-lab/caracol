BEGIN;

CREATE TYPE api_type_new AS ENUM (
    'grafanacloud',
    'elasticsearch',
    'cloudwatch'
);

ALTER TABLE providers
    ALTER COLUMN api_type TYPE api_type_new
        USING api_type::text::api_type_new;

DROP TYPE api_type;

ALTER TYPE api_type_new RENAME TO api_type;

COMMIT;