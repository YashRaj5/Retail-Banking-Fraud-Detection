# Databricks notebook source
# MAGIC %md 
# MAGIC ## Fraud Analysis on the Lakehouse

# COMMAND ----------

# DBTITLE 1,Top geographical flow of transactions > $350000
# MAGIC %sql
# MAGIC select * from (
# MAGIC   select
# MAGIC     countryOrig,
# MAGIC     countryDest,
# MAGIC     type,
# MAGIC     count(amount) as value
# MAGIC   from hive_metastore.fsi_fraud_detection.gold_transactions
# MAGIC   where amount > 350000
# MAGIC   group by countryOrig, countryDest, type
# MAGIC ) order by value desc
# MAGIC limit 20;

# COMMAND ----------

# DBTITLE 1,Hourly Fraud Amounts in the past 30 days
# MAGIC %sql
# MAGIC select ts, count(1) nbFraudTransactions, sum(amount) sumFraudTransactions--, label 
# MAGIC from(
# MAGIC     select from_unixtime(unix_timestamp(current_timestamp()) + (step * 3600)) ts, amount--, label
# MAGIC     from dbdemos.fsi_fraud_detection.gold_transactions
# MAGIC     join (--just getting the last 30 days
# MAGIC         select max(step)-(24*30) windowStart, max(step) windowEnd 
# MAGIC         from hive_metastore.fsi_fraud_detection.gold_transactions
# MAGIC     ) on step >= windowStart and step <= windowEnd
# MAGIC     where is_fraud
# MAGIC ) group by ts--, label
# MAGIC order by ts
