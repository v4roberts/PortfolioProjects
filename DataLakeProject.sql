-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Analyzing Oklahoma Electric Utility Companies and Energy Consumption Patterns by County and Sector

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### This project implements a Databricks pipeline that refreshes two datasets—U.S. electric utility rates and energy consumption data—using Delta Lake architecture. The pipeline processes raw data in the Bronze database, cleans and enriches the data in the Silver database, and stores facts and dimensions in the Gold database. Both datasets are integrated using a common dimension (county), allowing for detailed analysis of energy consumption and CO2 emissions across the U.S. at a granular level.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Load these files into databricks:
-- MAGIC -  https://catalog.data.gov/dataset/u-s-electric-utility-companies-and-rates-look-up-by-zipcode-2022
-- MAGIC   > We used the IOU rates with zip codes 2022.csv, These are investor owned utilities. These are private, for-profit companies that provide electricity. They are typically regulated by public utility commissions (PUCs) in each state and are owned by shareholders.
-- MAGIC -  https://maps.nrel.gov/slope/data-viewer?filters=%5B%5D&layer=energy-consumption.electricity-and-natural-gas-dollars-spent&year=2020&res=state
-- MAGIC     > This particular website, you'll have to search for "Electricity and Natural Gas Dolalrs Spen" under Data Library. Click on the Control, select State from the options and ensure to have year set to 2020. Then click on the arrow pointing down to download data.
-- MAGIC
-- MAGIC - https://www.unitedstateszipcodes.org/zip-code-database/
-- MAGIC     > This particular site we had download the CSV file under the "FREE" column on the right hand side. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating the Data Lake Architecture
-- MAGIC
-- MAGIC The following SQL code creates three databases named `bronze`, `silver`, and `gold` if they do not already exist. These databases are typically used in a data warehousing context to organize data into different layers based on the level of processing and transformation applied to the data.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Preparing the data for Transformation
-- MAGIC
-- MAGIC The following PySpark code reads several CSV files into DataFrames, cleans column names, unpivots the energy DataFrame, maps sector values to more descriptive names, and then saves the processed DataFrames as tables in a Delta Lake format. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Upon loading our dataset, we encountered errors due to invalid column names caused by hidden spaces or special characters. Although these issues were not immediately visible in Excel, ChatGPT helped us by defining a function that automatically cleaned the column names by replacing spaces and invalid characters with underscores.We needed to unpivot some columns in the Energy dataset, but the SQL UNPIVOT function was not successful for our use case. Instead, we used Python code with the stack function to perform the unpivot operation efficiently. Before saving the datasets to our Bronze Delta tables, we applied the column-cleaning function to both DataFrames to ensure compatibility and avoid further problems....
-- MAGIC
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC
-- MAGIC # Function to clean column names by replacing spaces and invalid characters
-- MAGIC def clean_column_names(df):
-- MAGIC     for col in df.columns:
-- MAGIC         cleaned_col = col.replace(' ', '_').replace('(', '').replace(')', '').replace(';', '').replace(',', '')
-- MAGIC         df = df.withColumnRenamed(col, cleaned_col)
-- MAGIC     return df
-- MAGIC
-- MAGIC #Read CSV into Spark DataFrame
-- MAGIC df_consumption = spark.read.csv("dbfs:/FileStore/tables/energy_consumption.csv", header=True)
-- MAGIC df_Energy = spark.read.csv("dbfs:/FileStore/iou_zipcodes_2022__1_.csv", header=True)
-- MAGIC df_zip = spark.read.csv("dbfs:/FileStore/zip_code_database.csv",header = True)
-- MAGIC
-- MAGIC # Clean column names to remove spaces/invalid characters
-- MAGIC df_consumption = clean_column_names(df_consumption)
-- MAGIC df_Energy = clean_column_names(df_Energy)
-- MAGIC df_zip= clean_column_names(df_zip)
-- MAGIC
-- MAGIC # Unpivot the DataFrame using stack function
-- MAGIC df_unpivoted = df_Energy.select(
-- MAGIC     'zip', 'eiaid', 'utility_name', 'state', 'service_type', 'ownership',
-- MAGIC     expr("stack(3, 'comm_rate', comm_rate, 'ind_rate', ind_rate, 'res_rate', res_rate) as (sector, rate)")
-- MAGIC )
-- MAGIC
-- MAGIC # Mapping sector values to new names
-- MAGIC df_unpivoted = df_unpivoted.withColumn(
-- MAGIC     'sector', 
-- MAGIC     F.when(F.col('sector') == 'comm_rate', 'commercial')
-- MAGIC      .when(F.col('sector') == 'ind_rate', 'industrial')
-- MAGIC      .when(F.col('sector') == 'res_rate', 'residential')
-- MAGIC )
-- MAGIC
-- MAGIC #automatic schema merging
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
-- MAGIC
-- MAGIC # Save the unpivoted DataFrame directly as bronze table
-- MAGIC df_unpivoted.write.option("mergeSchema", "true").option("overwriteSchema", "true").mode('overwrite').saveAsTable('bronze.energy')
-- MAGIC
-- MAGIC #consumption DataFrame to bronzetable
-- MAGIC df_consumption.write.option("mergeSchema", "true").mode('overwrite').saveAsTable('bronze.consumption')
-- MAGIC df_zip.write.saveAsTable('bronze.zip', mode = 'overwrite')
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Bronze Zip Code Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `zip` in the `bronze` schema by selecting specific columns from an existing table also named `zip` within the same schema.

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.zip AS
Select 
zip,
state,
county

FROM bronze.zip
WHERE state = 'OK'
--setting up for our dim_zip 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Silver Zip Code Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `zip` in the `silver` schema by selecting distinct values of specific columns from the `bronze.zip` table.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.zip AS
select 
distinct zip, 
county

 from bronze.zip
 ORDER BY zip asc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Gold Zip Code Dimension Table
-- MAGIC
-- MAGIC The following SQL code is used to create a new table called `dim_zip` in the `gold` schema. This table is constructed by selecting distinct zip codes and cleaning up the county names from the existing `silver.zip` table.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_zip AS
select 
distinct zip,
REPLACE(county, ' County', '') as county
 from silver.zip

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Silver Energy Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `energy` in the `silver` schema. This table is populated with specific columns related to energy data, filtered to include only records from the state of Oklahoma. The code performs a left join with the `gold.dim_zip` table to incorporate county information based on the zip code.

-- COMMAND ----------

--Created the Silver table for Energy, bringing only in 6 columns filtered by Oklahoma State
CREATE OR REPLACE TABLE silver.energy AS
SELECT
    e.zip,
    e.state, 
    z.county,
    e.utility_name, 
    e.sector,
    e.rate
FROM bronze.energy e
left join gold.dim_zip z on e.zip = z.zip
WHERE e.state = 'OK'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Bronze County Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `county` in the `bronze` schema. This table is populated with distinct county names and their corresponding state names, filtered to include only counties from the state of Oklahoma.

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.county AS
select distinct County_Name, State_Name
 from bronze.consumption
WHERE State_Name = 'Oklahoma'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Silver County Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `county` in the `silver` schema. This table is populated with distinct county names sourced from the previously created `bronze.county` table.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.county AS
SELECT DISTINCT County_Name
FROM bronze.county



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Gold County Dimension Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `dim_county` in the `gold` schema. This table includes all columns from the `silver.county` table along with an additional column that generates a unique key for each county.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_county AS
select *, MONOTONICALLY_INCREASING_ID() as county_key
from silver.county

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Bronze Date Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `date` in the `bronze` schema. This table is populated with distinct years sourced from the `bronze.consumption` table, specifically filtering for the year 2022.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.date
AS
select distinct year from bronze.consumption
where year = '2022'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Silver Date Dimension Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `dim_date` in the `silver` schema. This table includes all columns from the previously created `bronze.date` table and adds an additional column that generates a unique key for each date entry.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.dim_date
AS
SELECT 
*, 
MONOTONICALLY_INCREASING_ID() as date_key
FROM bronze.date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating our silver.consumption table. Joining with dim_date table to bring in only 2022 data to match Energy table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Silver Consumption Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `consumption` in the `silver` schema. This table is populated with filtered and transformed consumption data from the `bronze.consumption` table, specifically focusing on energy consumption in Oklahoma for the year 2022. 

-- COMMAND ----------

/* to match the silver.energy table, we concatenated the county name with county. The Energy dataset we downloaded is also only from 2022*/
CREATE OR REPLACE TABLE silver.consumption
AS
select 
County_Name,
--State_Name,
CASE WHEN State_Name = "Oklahoma" then "OK" else State_Name end as State,
Sector,
Source,
Consumption_MMBtu as yearly_Consumption_MMBtu,
Expenditure_US_Dollars as yearly_Expenditure_US_Dollars,
c.year
 from bronze.consumption c
 LEFT JOIN silver.dim_date d on d.year = c.year 
 where 1=1
 and State_Name = "Oklahoma"
 and c.Consumption_MMBtu != 0
 and d.year = '2022'
 ORDER by yearly_Consumption_MMBtu,county_name desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Gold Energy Consumption Fact Table
-- MAGIC
-- MAGIC The following SQL code creates a new table named `fact_energyconsumption` in the `gold` schema. This table aggregates energy consumption data, including costs and estimated CO2 emissions, for counties in Oklahoma based on the previously defined dimensions and consumption data.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_energyconsumption AS
select 
dc.county_key, 
--c.county_name,
c.sector,
c.source, 
ROUND(c.yearly_Consumption_MMBtu,0) as yearly_MMbtu,
ROUND(c.yearly_Expenditure_US_Dollars,0) as yearly_Total_Cost_USD,
e.utility_name,
ROUND(e.rate,4) as price_per_kWh,
(yearly_Total_Cost_USD / yearly_MMbtu) AS cost_per_MMbtu,
 yearly_MMbtu * 293.071 AS yearly_kWh,
  CASE 
        WHEN source = 'elec' THEN yearly_MMbtu * 114.36  -- CO2 emissions in kg per MMBtu for electricity
        WHEN source = 'ng' THEN yearly_MMbtu * 53.06    -- Example value for natural gas
        -- Add other sources and factors as needed
        ELSE 0
    END AS estimated_CO2_emissions_kg
from silver.consumption c
left join gold.dim_county dc on dc.County_Name = c.County_Name
left join silver.energy e on e.county = c.County_Name and e.sector = c.Sector
left join gold.dim_zip dz on dz.zip = e.zip
where c.State = 'OK'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average Energy Consumption by Sector
-- MAGIC
-- MAGIC The following SQL code calculates the average energy consumption in million British thermal units (MMBtu) for each energy sector in the `fact_energyconsumption` table. It retrieves distinct sectors and their corresponding average MMBtu values, grouped by sector.

-- COMMAND ----------

select 
distinct ec.sector,
AVG(ec.yearly_MMbtu) avg_MMbtu



from gold.fact_energyconsumption ec
left join gold.dim_county dc on dc.County_key = ec.County_key
GROUP BY ec.sector

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average CO2 Emissions by Utility Company
-- MAGIC
-- MAGIC The following SQL code calculates the average estimated CO2 emissions (in kilograms) for each utility provider in the `fact_energyconsumption` table. It retrieves the utility names along with their corresponding average emissions, grouped by utility name.

-- COMMAND ----------

select 
ec.utility_name,
AVG(ec.estimated_CO2_emissions_kg)


from gold.fact_energyconsumption ec
left join gold.dim_county dc on dc.County_key = ec.County_key
where ec.utility_name is not null
GROUP BY ec.utility_name


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top 10 Total Energy Expenditures Each Year by County
-- MAGIC
-- MAGIC The following SQL code calculates the total energy expenditure (in US dollars) for each county based on data from the `fact_energyconsumption` table. It retrieves the county names along with their total costs, sorted in descending order, and limits the results to the top ten counties with the highest expenditures.

-- COMMAND ----------

select 
dc.County_Name,
SUM(ec.yearly_Total_Cost_USD)


from gold.fact_energyconsumption ec
left join gold.dim_county dc on dc.County_key = ec.County_key
group by dc.County_Name
ORDER BY SUM(ec.yearly_Total_Cost_USD) desc
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top 10 Average CO2 Emmission Each Year by County
-- MAGIC
-- MAGIC The following SQL code calculates the average estimated CO2 emissions (in kilograms) for each county based on data from the `fact_energyconsumption` table. It retrieves the county names along with their average emissions, sorted in descending order, and limits the results to the top ten counties with the highest average emissions.

-- COMMAND ----------

select 
dc.County_Name,
AVG(ec.estimated_CO2_emissions_kg)


from gold.fact_energyconsumption ec
left join gold.dim_county dc on dc.County_key = ec.County_key

GROUP BY dc.County_Name
ORDER BY  AVG(ec.estimated_CO2_emissions_kg) desc
LIMIT 10
