BEGIN;

CREATE TYPE auth_type_new AS ENUM (
    'bearer_token',
    'basic_auth',
    'aws_access_key'
);

ALTER TABLE providers
    ALTER COLUMN auth_type TYPE auth_type_new
        USING auth_type::text::auth_type_new;

DROP TYPE auth_type;

ALTER TYPE auth_type_new RENAME TO auth_type;

COMMIT;