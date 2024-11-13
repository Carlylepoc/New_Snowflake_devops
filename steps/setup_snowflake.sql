USE ROLE ACCOUNTADMIN;

-- Separate database for git repository
CREATE OR ALTER DATABASE ENT_GIS_DEV;


-- API integration is needed for GitHub integration
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Carlylepoc') -- INSERT YOUR GITHUB USERNAME HERE
  ENABLED = TRUE;


-- Git repository object is similar to external stage
CREATE OR REPLACE GIT REPOSITORY ENT_GIS_DEV.public.quickstart_repo
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/Carlylepoc/new_snowflake_devops_demo'; -- INSERT URL OF FORKED REPO HERE


CREATE OR ALTER DATABASE ENT_GIS_PROD;


-- To monitor data pipeline's completion
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;


-- Database level objects
use database ent_gis_dev;
CREATE OR ALTER SCHEMA bronze;
CREATE OR ALTER SCHEMA silver;
CREATE OR ALTER SCHEMA gold;


use database ENT_GIS_DEV;
use schema bronze;

CREATE TABLE investors (
investor_id STRING PRIMARY KEY, -- Unique identifier for the investor
investor_name STRING, -- Name of the investor
email STRING, -- Investor's email address
phone_number STRING, -- Investor's phone number
date_of_birth DATE, -- Investor's date of birth
investment_goal STRING, -- Investor's investment goal (e.g., growth, income, balanced)
risk_tolerance STRING, -- Investor's risk tolerance (e.g., low, medium, high)
onboard_date DATE -- Date when the investor onboarded
);

INSERT INTO investors
(investor_id, investor_name, email, phone_number, date_of_birth, investment_goal, risk_tolerance, onboard_date)
VALUES
('INV12345', 'John Doe', 'john.doe@example.com', '123-456-7890', '1990-05-15', 'Growth', 'High', '2024-11-13'),
('INV12346', 'Jane Smith', 'jane.smith@example.com', '987-654-3210', '1985-02-20', 'Income', 'Medium', '2024-11-12');









