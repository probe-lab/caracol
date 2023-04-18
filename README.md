# Caracol

Caracol is a service for aggregating time series from external data systems.

## Database

### Creating the database and initial users

Create a database and a user that has permissions to access it. 

The following sql is an example that creates a database and a user called caracol:

	-- Create the database
	CREATE DATABASE caracol;

	-- Create the owner role
	CREATE ROLE caracol;
	GRANT caracol TO postgres;

	ALTER DATABASE caracol OWNER TO caracol;
	GRANT ALL PRIVILEGES ON SCHEMA public TO caracol;
	GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO caracol;

	-- The following is run substituting the passwords 
	-- ALTER ROLE caracol WITH LOGIN PASSWORD '<replace me>';
	ALTER ROLE caracol_ro WITH LOGIN PASSWORD '<replace me>';


### Creating and updating the schema


This project uses [tern](https://github.com/jackc/tern) to manage the database schema.

Install using `go install github.com/jackc/tern/v2@latest`

To update the database to the latest schema run `tern migrate` in the `migrations` directory.

Supply database details through environment variables or on the command line:

	tern migrate --conn-string postgres://user:password@hostname:5432/caracol?sslmode=disable

