# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Transaction Summary Notebook
# MAGIC
# MAGIC This notebook creates aggregated transaction summaries for analytics.
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Reads transaction data from the source table
# MAGIC - Aggregates transactions by date and type
# MAGIC - Writes summarized results to the transactions_summary table
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name
# MAGIC
# MAGIC **Dependencies:**
# MAGIC - Runs after the `customer_360` task in the bakehouse_data_refresh job
# MAGIC - Expects transactions table to exist at `{catalog}.{schema}.transactions`

# COMMAND ----------

# DBTITLE 1,Read transaction data
# Read transaction data from source table
transactions_df = spark.table(f"{catalog}.{schema}.transactions")

# Display sample to verify data
display(transactions_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Create aggregated summaries
from pyspark.sql import functions as F

# Create daily transaction summaries
transaction_summary = (
    transactions_df
    .withColumn("transaction_date", F.to_date("transaction_timestamp"))
    .groupBy(
        "transaction_date",
        "transaction_type"
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount")
    )
    .orderBy("transaction_date", "transaction_type")
)

# Display summary results
display(transaction_summary)

# COMMAND ----------

# DBTITLE 1,Write summary to target table
# Write the transaction summary to the target table
target_table = f"{catalog}.{schema}.transactions_summary"

(
    transaction_summary
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"✓ Successfully wrote {transaction_summary.count()} summary records to {target_table}")