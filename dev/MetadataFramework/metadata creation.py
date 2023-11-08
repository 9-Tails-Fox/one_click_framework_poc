# Databricks notebook source
"""This notebook wil be used to create metaadata json by passing values in the parameters
"""

# COMMAND ----------

# DBTITLE 1,Declare Widgets
dbutils.widgets.text("Source_Name","none","Source Name")
dbutils.widgets.dropdown("Source_Extension","csv",["csv","json","parquet","excel"])
dbutils.widgets.text("Source_adls_path","none","Source adls path")
# dbutils.widgets.rm("Raw destination adls path")
dbutils.widgets.text("Raw_target_schema","none","Raw target schema")
dbutils.widgets.text("Raw_target_table","none","Raw target table")
dbutils.widgets.text("Silver_target_schema","none","Silver target schema")
dbutils.widgets.text("Silver_target_table","none","Silver target table")
# dbutils.widgets.text("Silver_destination_adls_path","none","Silver destination adls path")
dbutils.widgets.text("SCD_type","none","SCD type")
dbutils.widgets.text("Primary_key","none","PrimaryKey: ['col1', 'col2', 'col3']")
dbutils.widgets.dropdown("initial_load", "none", ["FullLoad","DeltaLoad","none"])
dbutils.widgets.text("run_flag","none","run_flag")
dbutils.widgets.text("silver_schema","none","SilverSchema: ['col1', cast('col3' as int)]")

# COMMAND ----------

# DBTITLE 1,Fetch Widgets values
source_name=dbutils.widgets.get("Source_Name")
source_extension=dbutils.widgets.get("Source_Extension")
source_adls_path=dbutils.widgets.get("Source_adls_path")
# raw_destination_adls_path=dbutils.widgets.get("Raw_destination_adls_path")
raw_target_schema=dbutils.widgets.get("Raw_target_schema")
raw_target_table=dbutils.widgets.get("Raw_target_table")
silver_target_schema=dbutils.widgets.get("Silver_target_schema")
silver_target_table=dbutils.widgets.get("Silver_target_table")
# silver_destination_adls_path=dbutils.widgets.get("Silver_destination_adls_path")
scd_type=dbutils.widgets.get("SCD_type")
primary_keys=dbutils.widgets.get("Primary_key")
initial_load=dbutils.widgets.get("initial_load")
run_flag=dbutils.widgets.get("run_flag")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

checkpoint_path = "none"
scd_order_by = ""
raw_destination_adls_path = f"dbfs:/mnt/metadata_framework/raw/{raw_target_table}/"
silver_destination_adls_path = f"dbfs:/mnt/metadata_framework/silver/{raw_target_table}/"
# primary_keys = ['','customer_id'] ## 'invoice_no'

# COMMAND ----------

# DBTITLE 1,Declare metadata function 
def metadata(source_name= None, source_extension= None, source_adls_path= None, checkpoint_path= None, raw_destination_path= None, raw_target_schema= None, raw_target_table= None, silver_target_schema= None,silver_target_table= None, silver_destination_path= None, scd_type= None, scd_order_by= None, primary_keys= None, initial_load= None, run_flag= None, silver_schema= None):
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
            "primary_keys":eval(primary_keys),
            "initial_load":f"{initial_load}",
            "run_flag":f"{run_flag}",
            "silver_schema": eval(silver_schema)
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
    silver_schema
)

# COMMAND ----------

# DBTITLE 1,display metadata records
df=(spark.read.format("json").load("dbfs:/FileStore/metadata"))

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.read.format('csv').option('header','true').load('dbfs:/mnt/metadata_framework/ADLS/Fact_tables/sales_data/sales_data.csv')
#invoice_no

# COMMAND ----------

df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

['cast(invoice_no as int)', 'customer_id', 'category', 'cast(quantity as double)', 'cast(price as decimal(15,2))', 'cast(invoice_date as date)', 'shopping_mall', 'cast(Effective_TimeStamp as timestamp )']
