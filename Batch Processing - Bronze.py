# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Bronze Layer
# MAGIC
# MAGIC We are trying to get the data from the datasources, consolidate it and store it in our workspace to process it in silver layer. Data is consumed in csv.gz format and stored in parquet format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "batch_processing_bronze_ingest"

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
# MAGIC ## Read OCPP Data
# MAGIC We've done this a couple of times before! Run the following cells to download the data to local storage and create a DataFrame from it.

# COMMAND ----------

# url1 = "https://github.com/vinodmurug/Heka_Dataset/raw/main/Adware_before_reboot_Cat.csv"
# filepath1 = helpers.download_to_local_dir(url1)
# url2 = "https://github.com/vinodmurug/Heka_Dataset/raw/main/Backdoor_before_reboot_Cat.csv"
# filepath2 = helpers.download_to_local_dir(url2)
url1 = "https://github.com/vinodtkn/MedallionArchitecture/blob/main/heka_dataset.csv.gz"
filepath1 = helpers.download_to_local_dir(url1)

# COMMAND ----------

dbutils.fs.ls("/FileStore/abhinavsuman0204/batch_processing_bronze_ingest/")

# COMMAND ----------

# from pyspark.sql.functions import col,lit
# from pyspark.sql import DataFrame
# from functools import reduce
filepath1 = "dbfs:/user/hive/warehouse/heka_dataset"



# df1 = spark.read.format("csv") \
#         .option("header", True) \
#         .option("delimiter", ",") \
#         .option("escape", "\\") \
#         .load(filepath1)
# print(df1.count())
# display(df1)
# df2 = spark.read.format("csv") \
#         .option("header", True) \
#         .option("delimiter", ",") \
#         .option("escape", "\\") \
#         .load(filepath2)
# print(df2.count())
# display(df2)

# all_dfs=df1.union(df2)
# print(all_dfs.count())
# display(all_dfs)
from pyspark.sql.functions import col,lit
from pyspark.sql import DataFrame
from functools import reduce

# Define the file path

# Read the gzip-compressed CSV file
try:
    df1 = spark.read.format("delta").load(filepath1)
    print("Number of rows in df1:", df1.count())
    display(df1)
except Exception as e:
    print("Error reading the file:", e)

# Drop the "_c0" column from df1
all_dfs = df1.drop("_c0")
print(all_dfs.count())
display(all_dfs)


# COMMAND ----------

def write(input_df: DataFrame):
    out_dir = f"{working_directory}/output_bronze/"
    print(f"Your current working directory is: {out_dir}")
    
    ### YOUR CODE HERE ###
    mode_name = "overwrite"
    ###
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)
    
write(all_dfs)

# COMMAND ----------


