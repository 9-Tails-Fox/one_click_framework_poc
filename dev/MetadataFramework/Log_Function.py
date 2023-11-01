# Databricks notebook source
# DBTITLE 1,log function 
""""
accepts the log parameters and write logs as json in dbfs:/FileStore/metadata_logs/ folder
"""

def log_table_changes(Source_path, Raw_target_table, Raw_destination_path, Silver_target_table, Silver_destination_path, Start_timestamp, End_timestamp, job_trigger_time, task_notebook_path, job_run_id, job_name, job_id, job_start_time, workspace_url, task_run_id, workspace_id):
    df = spark.createDataFrame([
        {
            "job_trigger_time": f"{job_trigger_time}",
            "task_notebook_path": f"{task_notebook_path}",
            "job_run_id": f"{job_run_id}",
            "job_name": f"{job_name}",
            "job_id": f"{job_id}",
            "job_start_time": f"{job_start_time}",
            "workspace_url": f"{workspace_url}",
            "task_run_id": f"{task_run_id}",
            "workspace_id": f"{workspace_id}",
            "Source_path":f"{Source_path}",
            "Raw_target_table_name":f"{Raw_target_table}",
            "Raw_destination_path": f"{Raw_destination_path}",
            # "Raw_layer_status": f"{}",
            "Silver_target_table": f"{Silver_target_table}",
            "Silver_destination_path":f"{Silver_destination_path}",
            # "Silver_layer_status":f"{}",
            "Start_timestamp":f"{Start_timestamp}",
            "End_timestamp":f"{End_timestamp}"
        }
    ])
    df.write.mode("append").format('json').save("dbfs:/FileStore/metadata_logs/")
