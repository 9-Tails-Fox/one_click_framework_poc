# Databricks notebook source
"""This notebook wil be used to create metaadata json by passing values in the parameters
"""

# COMMAND ----------

# DBTITLE 1,Declare Widgets
dbutils.widgets.text("Source_Name","none","Source Name")
dbutils.widgets.text("Source_Extension","none","Source Extension")
dbutils.widgets.text("Source_adls_path","none","Source adls path")
dbutils.widgets.text("Raw_destination_adls_path","none","Raw destination adls path")
dbutils.widgets.text("Raw_target_schema","none","Raw target schema")
dbutils.widgets.text("Raw_target_table","none","Raw target table")
dbutils.widgets.text("Silver_target_schema","none","Silver target schema")
dbutils.widgets.text("Silver_target_table","none","Silver target table")
dbutils.widgets.text("Silver_destination_adls_path","none","Silver destination adls path")
dbutils.widgets.text("SCD_type","none","SCD type")
dbutils.widgets.text("Primary_key","none","Primary key")
dbutils.widgets.text("initial_load","none","initial load")
dbutils.widgets.text("run_flag","none","run_flag")

# COMMAND ----------

# DBTITLE 1,Fetch Widgets values
source_name=dbutils.widgets.get("Source_Name")
source_extension=dbutils.widgets.get("Source_Extension")
source_adls_path=dbutils.widgets.get("Source_adls_path")
raw_destination_adls_path=dbutils.widgets.get("Raw_destination_adls_path")
raw_target_schema=dbutils.widgets.get("Raw_target_schema")
raw_target_table=dbutils.widgets.get("Raw_target_table")
silver_target_schema=dbutils.widgets.get("Silver_target_schema")
silver_target_table=dbutils.widgets.get("Silver_target_table")
silver_destination_adls_path=dbutils.widgets.get("Silver_destination_adls_path")
scd_type=dbutils.widgets.get("SCD_type")
primary_key=dbutils.widgets.get("Primary_key")
initial_load=dbutils.widgets.get("initial_load")
run_flag=dbutils.widgets.get("run_flag")

# COMMAND ----------

# DBTITLE 1,Declare metadata function 
def metadata(source_name, source_extension, source_adls_path, checkpoint_path, raw_destination_path, raw_target_schema, raw_target_table, silver_target_schema,silver_target_table, silver_destination_path, scd_type, scd_order_by, primary_keys, initial_load, run_flag):
    metadata_df  = spark.createDataFrame([
        {
            "source_name": f"{source_name}",
            "source_extension": f"{source_extension}",
            "source_path": f"{source_adls_path}",
            "checkpoint_path": f"{checkpoint_path}",
            "raw_destination_path": f"{raw_destination_path}",
            "raw_target_schema":f"{raw_target_schema}",
            "raw_target_table": f"{raw_target_table}",
            "silver_target_schema": f"{silver_target_schema}",
            "silver_target_table": f"{silver_target_table}",
            "silver_destination_path": f"{silver_destination_path}",
            "scd_type": f"{scd_type}",
            "scd_order_by": f"{scd_order_by}",
            "primary_keys":f"{primary_keys}",
            "initial_load":f"{initial_load}",
            "run_flag":f"{run_flag}"
        }
    ])
    metadata_df.write.mode("append").format("json").save("dbfs:/FileStore/metadata")
    print("row inserted")

# COMMAND ----------

# DBTITLE 1,metadata record
metadata(
    source_name,
    source_extension,
    source_adls_path,
    checkpoint_path,
    raw_destination_adls_path,
    raw_target_schema,
    raw_target_table,
    silver_target_schema,
    silver_target_table,
    silver_destination_adls_path,
    scd_type,
    scd_order_by,
    primary_keys,
    initial_load,
    run_flag,
)

# COMMAND ----------

# DBTITLE 1,display metadata records
display(spark.read.format("json").load("dbfs:/FileStore/metadata"))
