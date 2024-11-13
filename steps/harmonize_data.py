import os

from snowflake.core import Root, CreateMode
from snowflake.snowpark import Session
from snowflake.core.view import View, ViewColumn

# Pipeline to harmonize the data (creating a new view)
pipeline = [
View(
name="Investors",
columns=[
ViewColumn(name="investor_id"),
ViewColumn(name="investor_name"),
ViewColumn(name="onboard_date"),
ViewColumn(name="investor_age")
],
query="""
SELECT 
investor_id,
investor_name,
onboard_date, 
DATEDIFF(YEAR, onboard_date, CURRENT_DATE()) AS investor_age 
FROM ENT_GIS_DEV.bronze.investors;
"""
)
]
# Entry point for PythonAPI
session = Session.builder.configs({"sfDatabase": "ENT_GIS_DEV"}).create()
# Create views in the Silver schema
silver_schema = session.database("ENT_GIS_PROD").schema("silver")
for view in pipeline:
    silver_schema.create_or_replace_view(view)