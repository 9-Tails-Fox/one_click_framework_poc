# Databricks notebook source
# DBTITLE 1,fetch parameters from jobs
"""
fetching parameters from jobs 

"""

job_trigger_time = dbutils.widgets.get("job_trigger_time")
task_notebook_path = dbutils.widgets.get("task_notebook_path")
job_run_id = dbutils.widgets.get("job_run_id")
job_name = dbutils.widgets.get("job_name")
job_id = dbutils.widgets.get("job_id")
job_start_time = dbutils.widgets.get("job_start_time")
workspace_url = dbutils.widgets.get("workspace_url")
task_run_id = dbutils.widgets.get("task_run_id")
workspace_id = dbutils.widgets.get("workspace_id")

# COMMAND ----------

# DBTITLE 1,calling transformation notebooks
# MAGIC %run "/Repos/suraj.barge@celebaltech.com/one_click_framework_poc/dev/MetadataFramework/bronze_to_silver"

# COMMAND ----------

# MAGIC %run "/Repos/suraj.barge@celebaltech.com/one_click_framework_poc/dev/MetadataFramework/Log_Function"

# COMMAND ----------

# DBTITLE 1,main run_metadata_framework function 
def run_metadata_framework(job_trigger_time: None, task_notebook_path: None, job_run_id: None, job_name: None, job_id: None, job_start_time: None, workspace_url: None, task_run_id: None, workspace_id: None):
    """
       run_metadata_framework reads the metadata json and loops over each row and run the code for bronze and silver table
    """
    import datetime
    try:
        for row in spark.read.format("json").load("dbfs:/FileStore/metadata").collect():

            starttime = datetime.datetime.now()

            readData(row.source_path, row.raw_target_schema, row.raw_target_table,row.raw_destination_path,row.initial_load)
            print(f"{ row.raw_target_table} raw completed")

            writeDatatoSilver(row.silver_target_schema, row.silver_target_table, row.silver_destination_path, row.raw_target_schema, row.raw_target_table,row.primary_keys,row.initial_load)
            print(f"{row.silver_target_table} silver completed")

            endtime = datetime.datetime.now()

            log_table_changes(row.source_path ,row.raw_target_table, row.raw_destination_path,row.silver_target_table, row.silver_destination_path,starttime, endtime, job_trigger_time, task_notebook_path, job_run_id, job_name, job_id, job_start_time, workspace_url, task_run_id, workspace_id)

    except Exception as e:
        print(f'Error : {e}')

run_metadata_framework(job_trigger_time, task_notebook_path, job_run_id, job_name, job_id, job_start_time, workspace_url, task_run_id, workspace_id)
