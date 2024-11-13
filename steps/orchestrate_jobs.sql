-- Create a stream to track changes in the source table
-- use database ENT_GIS_DEV;
-- use schema silver;
-- CREATE OR REPLACE STREAM investors_stream ON TABLE ENT_GIS_DEV.silver.investors
-- SHOW_INITIAL_ROWS = TRUE;

use database ENT_GIS_PROD;
use schema gold;
--- Data transformation task
CREATE OR REPLACE TASK transform_investors_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1440 MINUTE'
AS
INSERT INTO ENT_GIS_PROD.gold.transformed_investors
SELECT
investor_id,
investor_name,
onboard_date,
investor_age
FROM ENT_GIS_DEV.silver.investors
WHERE onboard_date > (SELECT MAX(onboard_date) FROM ENT_GIS_PROD.gold.transformed_investors);

-- Email notification task
CREATE OR REPLACE TASK email_notification_task
WAREHOUSE = COMPUTE_WH
AFTER transform_investors_task
AS
BEGIN
LET new_data_count INT := (SELECT COUNT(*) FROM ENT_GIS_PROD.gold.transformed_investors WHERE onboard_date > CURRENT_DATE - 1);
-- Send email based on new data count
IF :new_data_count = 0 THEN
CALL SYSTEM$SEND_EMAIL(
'
peddapallys@hexaware.com
',
'No New Data Available',
'No new data was added to the transformed investors table today.'
);
ELSE
CALL SYSTEM$SEND_EMAIL(
'
peddapallys@hexaware.com
',
'Data Transformation Complete',
'Data transformation was successfully completed.'
);
END IF;
END;
-- Resume the email notification task to include in the DAG runs
ALTER TASK email_notification_task RESUME;
-- Manually execute the transformation task (if needed)
EXECUTE TASK transform_investors_task;