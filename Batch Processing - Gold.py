# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Gold Tier
# MAGIC
# MAGIC In this layer, we consume the data from Silver layer, transform it and use it for inferring data insights using visualization.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "heka_batch_processing_gold"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer
# MAGIC Let's read the parquet files that we created in the Silver layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Silver layer but for simplicity and consistent results [of this exercise], we're going to read in a Silver output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!

# COMMAND ----------

from pyspark.sql import DataFrame
df = spark.read.parquet("dbfs:/FileStore/abhinavsuman0204/heka_batch_processing_silver/output_silver/")
print(df.count())
display(df)

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import IntegerType
df_MaxProcess=df.withColumn("Process_total",fn.col("Process_total").cast(IntegerType())) \
.select('Category','Process_total').groupBy('Category').agg(fn.max('Process_total').alias('Total_Processes_Breached'))
display(df_MaxProcess)

# COMMAND ----------

# Define the path to save the gold layer
gold_layer_path = "/FileStore/abhinavsuman0204/heka_batch_processing_gold/output_gold/heka_dataset"



# Save the DataFrame to the gold layer
df_MaxProcess.write.format("delta").mode("overwrite").save(gold_layer_path)

# COMMAND ----------


