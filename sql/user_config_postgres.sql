-- Creating a read only user for postgres connection
-- Maintaining Data Security 
CREATE USER stageReader WITH PASSWORD 'elt-framework';
GRANT CONNECT ON DATABASE "ELT" TO stageReader;

-- Grant the necessary permissions for all the schemas necessary 
GRANT USAGE ON SCHEMA public, sfdc_stage, sfdc_ingestion_config TO stageReader;
GRANT SELECT ON ALL TABLES IN SCHEMA public, sfdc_stage, sfdc_ingestion_config TO stageReader;

-- Altering default privileges to maintain permissions for new tables 
ALTER DEFAULT PRIVILEGES IN SCHEMA public, sfdc_stage, sfdc_ingestion_config
GRANT SELECT ON TABLES TO stageReader;