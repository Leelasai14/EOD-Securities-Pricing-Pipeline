/*===============================================================
  Context & session setup
  - Choose a small, cheap warehouse for ingestion/transforms
  - Scope all operations to the target database
================================================================*/
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;



------------------------------------------------------------
-- SECTION 1: FILE FORMAT CONFIGURATION
-- Purpose: Define a reusable CSV file format to interpret staged files.
------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT SEC_PRICING.RAW.CSV_EOD
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');





------------------------------------------------------------
-- SECTION 2: STORAGE INTEGRATION (S3)
-- Step 1: In AWS, create an IAM role named 'snowflake_s3_integration_role'
--         and configure a trust relationship with Snowflake’s external ID.
-- Step 2: Use that IAM role ARN in the storage integration below.
-- This integration securely manages credentials for S3 access.
------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION INT_S3_AIRFLOWSNOWDE
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::863518449998:role/snowflake_s3_integration_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://securitiespricingfinal/market/bronze/');

-- Verify integration details (Snowflake provides an external ID and S3 policy template).
DESC INTEGRATION INT_S3_AIRFLOWSNOWDE;


------------------------------------------------------------
-- SECTION 3: EXTERNAL STAGE CREATION
-- Create a named external stage pointing to the S3 bucket.
-- The stage uses the above integration to access files securely.
------------------------------------------------------------


CREATE OR REPLACE STAGE SEC_PRICING.RAW.EXT_BRONZE
  URL = 's3://securitiespricingfinal/market/bronze/'
  STORAGE_INTEGRATION = INT_S3_AIRFLOWSNOWDE;

-- Verify the stage configuration and permissions.
DESC STAGE SEC_PRICING.RAW.EXT_BRONZE;

-- List all files currently available in the S3 stage.
LIST @SEC_PRICING.RAW.EXT_BRONZE;
