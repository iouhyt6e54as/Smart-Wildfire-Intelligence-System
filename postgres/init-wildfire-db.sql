-- Safe, idempotent initialization of the Wildfire application database
-- Executed automatically on container initialization by postgres entrypoint

DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'wildfire') THEN
      CREATE ROLE wildfire WITH LOGIN PASSWORD 'wildfire_secret';
   END IF;
END
$do$;

SELECT 'CREATE DATABASE wildfire OWNER wildfire'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'wildfire')\gexec

GRANT ALL PRIVILEGES ON DATABASE wildfire TO wildfire;
