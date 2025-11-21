# Databricks notebook source
spark.sql("""SELECT  
    *,
    UPPER(customer_name) AS customer_name_upper,
    date(current_timestamp) AS processed_date
FROM datamodelling.bronze.bronze_table""").createOrReplaceTempView('silver_source')

# COMMAND ----------

if spark.catalog.tableExists('datamodelling.silver.silver_table'):
    pass
    
else:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS datamodelling.silver.silver_table
        AS
        SELECT * FROM silver_source"""
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS datamodelling.silver.silver_table
# MAGIC AS
# MAGIC SELECT * FROM silver_source

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodelling.silver.silver_table
# MAGIC USING silver_source
# MAGIC ON datamodelling.silver.silver_table.order_id = silver_source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
