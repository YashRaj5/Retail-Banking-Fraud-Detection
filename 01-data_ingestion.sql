-- Databricks notebook source
-- DBTITLE 1,Let's explore our raw incoming transactions data
-- MAGIC %python
-- MAGIC display(spark.read.json('/tmp/fsi/fraud-detection/transactions'))

-- COMMAND ----------

-- DBTITLE 1,raw incoming customers data
-- MAGIC %python
-- MAGIC display(spark.read.csv('/tmp/fsi/fraud-detection/customers', header = True, multiLine=True))

-- COMMAND ----------

-- DBTITLE 1,raw incoming country data
-- MAGIC %python
-- MAGIC display(spark.read.csv('/tmp/fsi/fraud-detection/country_code', header=True))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Loading our data using `cloud_files`

-- COMMAND ----------

-- DBTITLE 1,Ingest transactions
CREATE INCREMENTAL LIVE TABLE bronze_transactions 
  COMMENT "Historical banking transaction to be trained on fraud detection"
AS 
  SELECT * FROM cloud_files("/tmp/fsi/fraud-detection/transactions", "json", map("cloudFiles.maxFilesPerTrigger", "1", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,ingest customers
CREATE STREAMING LIVE TABLE banking_customers (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
AS 
  SELECT * FROM cloud_files("/tmp/fsi/fraud-detection/customers", "csv", map("cloudFiles.inferColumnTypes", "true", "multiLine", "true"))

-- COMMAND ----------

-- DBTITLE 1,reference table
CREATE INCREMENTAL LIVE TABLE country_coordinates
AS 
  SELECT * FROM cloud_files("/tmp/fsi/fraud-detection/country_code", "csv")

-- COMMAND ----------

-- DBTITLE 1,Fraud report (labels for ML traiing)
CREATE INCREMENTAL LIVE TABLE fraud_reports
AS 
  SELECT * FROM cloud_files("/tmp/fsi/fraud-detection/fraud_report", "csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Enforcing data quality and materializing our tables for analysis

-- COMMAND ----------

-- DBTITLE 1,Silver
CREATE INCREMENTAL LIVE TABLE silver_transactions (
  CONSTRAINT correct_data EXPECT (id IS NOT NULL),
  CONSTRAINT correct_customer_id EXPECT (customer_id IS NOT NULL)
)
AS 
  SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data, f._rescued_data), 
          regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
          regexp_replace(countryDest, "\-\-", "") as countryDest, 
          newBalanceOrig - oldBalanceOrig as diffOrig, 
          newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(live.bronze_transactions) t
  LEFT JOIN live.fraud_reports f using(id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Aggregate and join data to create our ML features

-- COMMAND ----------

-- DBTITLE 1,Gold
CREATE LIVE TABLE gold_transactions (
  CONSTRAINT amount_decent EXPECT (amount > 10)
)
AS 
  SELECT t.* EXCEPT(countryOrig, countryDest, is_fraud), c.* EXCEPT(id, _rescued_data),
          boolean(coalesce(is_fraud, 0)) as is_fraud,
          o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
          d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
FROM live.silver_transactions t
  INNER JOIN live.country_coordinates o ON t.countryOrig=o.alpha3_code 
  INNER JOIN live.country_coordinates d ON t.countryDest=d.alpha3_code 
  INNER JOIN live.banking_customers c ON c.id=t.customer_id 
