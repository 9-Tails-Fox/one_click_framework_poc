# Databricks notebook source
# DBTITLE 1,readData function
"""
readData function accepts parameters from the metadata funciton and reads data from source path and write into the bronze layer target destination schema and cloud path
"""

def readData(source_path, raw_target_schema, raw_target_table,raw_destination_path):
    spark.sql(f"""
              create database if not exists {raw_target_schema};
              """)
    df = spark.read.format('csv').option('header','true').load(f"{source_path}")
    df.write.mode("overwrite").option("path",f"{raw_destination_path}").option('mergeSchema','true').saveAsTable(f"{raw_target_schema}.{raw_target_table}")
    

# COMMAND ----------

# DBTITLE 1,writeDatatoSilver function
"""
writeDatatoSilver function accepts parameters from the metadata funciton and reads data write into the silver layer target destination schema and cloud path
"""

def writeDatatoSilver(silver_target_schema, silver_target_table, silver_destination_path, raw_target_schema, raw_target_table):
    spark.sql(f"""
              create database if not exists {silver_target_schema};
              """)
    df = spark.read.table(f"{raw_target_schema}.{raw_target_table}")
    df.write.mode("overwrite").option("path",f"{silver_destination_path}").option('mergeSchema','true').saveAsTable(f"{silver_target_schema}.{silver_target_table}")
