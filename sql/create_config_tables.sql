-- Made By Karthik 
-- Create schemas
CREATE SCHEMA IF NOT EXISTS sfdc_ingestion_config;
CREATE SCHEMA IF NOT EXISTS sfdc_stage;

-- Table 1: ing_tbl_list
CREATE TABLE sfdc_ingestion_config.ing_tbl_list (
    src_tbl_nm VARCHAR(255) PRIMARY KEY,
    trg_schema VARCHAR(255) NOT NULL,
    trg_tbl_nm VARCHAR(255) NOT NULL,
    isenabled BOOLEAN DEFAULT TRUE,
    incr_col VARCHAR(255) -- e.g., LastModifiedDate
);

-- Table 2: ing_tbl_columns
CREATE TABLE sfdc_ingestion_config.ing_tbl_columns (
    src_tbl_nm VARCHAR(255),
    src_tbl_col VARCHAR(255),
    trg_tbl_nm VARCHAR(255),
    trg_tbl_col VARCHAR(255),
    trg_col_type VARCHAR(50) DEFAULT 'VARCHAR(255)',
    PRIMARY KEY (src_tbl_nm, src_tbl_col)
);

-- Table 3: ing_tbl_checkpoint
CREATE TABLE sfdc_ingestion_config.ing_tbl_checkpoint (
    trg_tbl_nm VARCHAR(255) PRIMARY KEY,
    last_fetch_ts TIMESTAMP WITH TIME ZONE DEFAULT '0001-01-01 00:00:00 UTC' -- Oldest possible timestamp
);

--Verifying
SELECT table_name, column_name, data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'sfdc_ingestion_config'
ORDER BY table_name, ordinal_position;