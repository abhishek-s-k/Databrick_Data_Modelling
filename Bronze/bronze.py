# Databricks notebook source
last_load_date = '2000-01-01'

# COMMAND ----------

if spark.catalog.tableExists("datamodelling.bronze.bronze_table"):
    last_load_date = spark.sql("SELECT max(order_date) FROM datamodelling.bronze.bronze_table").collect()[0][0]
else:
    last_load_date = '2000-01-01'

# COMMAND ----------

print(last_load_date)

# COMMAND ----------

spark.sql(
        f"""SELECT * FROM datamodelling.default.source_data
        WHERE order_date > '{last_load_date}'"""
    ).createOrReplaceTempView("bronze_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodelling.bronze.bronze_table AS
# MAGIC SELECT * FROM bronze_source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodelling.bronze.bronze_table
