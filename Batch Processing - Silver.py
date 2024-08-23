# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Tier
# MAGIC
# MAGIC In this layer, we cleanse the data input from bronze layer and store it for further processing in gold layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "heka_batch_processing_silver"

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
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Bronze layer but for simplicity and consistent results [of this exercise], we're going to read in a Bronze output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!.

# COMMAND ----------

from pyspark.sql import DataFrame
df = spark.read.parquet("dbfs:/FileStore/abhinavsuman0204/batch_processing_bronze_ingest/output_bronze/")
print(df.count())
display(df)


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df

filepath="dbfs:/FileStore/abhinavsuman0204/batch_processing_bronze_ingest/output_bronze/"    
df = read_parquet(filepath)

display(df)


# COMMAND ----------

import pyspark.sql.functions as fn
#df3=df.select('Category','Family','Process_total').groupBy('Category').agg(fn.max('Process_total').alias('Process_total_max'))
df=df.select('Category','Family','Process_total','Battery_wakelock','Memory_HeapSize','Memory_HeapAlloc','Memory_HeapFree')
df.show()

# COMMAND ----------

def write(input_df: DataFrame):
    out_dir = f"{working_directory}/output_silver/"
    print(f"Your current working directory is: {out_dir}")
    
    ### YOUR CODE HERE ###
    mode_name = "overwrite"
    ###
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)
    
write(df)

# COMMAND ----------


