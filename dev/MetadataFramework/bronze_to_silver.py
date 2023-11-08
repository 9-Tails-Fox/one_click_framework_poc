# Databricks notebook source
# DBTITLE 1,readData function
"""
readData function accepts parameters from the metadata funciton and reads data from source path and write into the bronze layer target destination schema and cloud path
"""

def readData(source_path, raw_target_schema, raw_target_table,raw_destination_path, load_type):

    df = spark.read.format('csv').option('inferSchema','true').option('header','true').load(f"{source_path}")
    
    if raw_target_schema not in [db.name for db in spark.catalog.listDatabases()]:
        spark.sql(f"""
                      create database if not exists {raw_target_schema};
                """)
    else:
        pass
    
    if raw_target_table not in [tb.name for tb in spark.catalog.listTables(f"{raw_target_schema}")]:
        spark.sql(f"""
                  create table if not exists {raw_target_schema}.{raw_target_table} 
                  location "{raw_destination_path}"
                """)
        spark.sql(f"""
                  alter table {raw_target_schema}.{raw_target_table} 
                  set tblproperties (delta.enableChangeDataFeed = true)
                """)
        df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(f"{raw_target_schema}.{raw_target_table}")

    else:
        if load_type == "FullLoad":
            df.write.mode('overwrite').option("path",f"{raw_destination_path}").option("mergeSchema", "True").  saveAsTable(f"{raw_target_schema}.{raw_target_table}")
            print(f'{raw_target_table} FullLoad complete')

        elif load_type == "DeltaLoad":
            df.write.mode("append").option("path",f"{raw_destination_path}").option('mergeSchema','true').  saveAsTable(f"{raw_target_schema}.{raw_target_table}")
            print(f'{raw_target_table} DeltaLoad complete')
            

# COMMAND ----------

# DBTITLE 1,writeDatatoSilver function
"""
writeDatatoSilver function accepts parameters from the metadata funciton and reads data write into the silver layer target destination schema and cloud path
"""

def return_update_and_pf_str(lis, lg):
    pat = ""
    trim = 0
    if lg == 'update':
        pat = pat+","
        trim = -2
    elif lg == 'pk':
        pat = pat+" and"
        trim = -4
    
    str_ = ""
    for i in lis:
        temp = f"t.{i} = s.{i}{pat} "
        str_ = str_+ temp
    str_ = str_[0: trim]
    return str_

def return_insert_sel_val_str(lis,lg):
    str_ = ""
    pat = ""
    if lg == "select":
        pat = pat+"t"
    elif lg == "value":
        pat = pat+"s"
    
    for i in lis:
        temp = f"{pat}.{i},"
        str_ = str_+temp
    str_ = str_[0:-1]
    return str_

def writeDatatoSilver(silver_target_schema, silver_target_table, silver_destination_path, raw_target_schema, raw_target_table, pk_col, load_type, silver_schema):


    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    df_raw = spark.read.table(f"{raw_target_schema}.{raw_target_table}")
    raw_columns = df_raw.columns

    df = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(f"{raw_target_schema}.{raw_target_table}"))

    windowSpec = Window.partitionBy(pk_col).orderBy(F.desc("Effective_TimeStamp"), F.desc('_commit_version'))
    df = df.withColumn("rank", F.dense_rank().over(windowSpec)).filter("rank = 1")
    df.selectExpr(silver_schema).createOrReplaceTempView(f"{raw_target_schema}_source_view")

    if silver_target_schema not in [db.name for db in spark.catalog.listDatabases()]:
        spark.sql(f"""
                      create database if not exists {silver_target_schema};
                """)
    else:
        pass

    if silver_target_table not in [tb.name for tb in spark.catalog.listTables(f"{silver_target_schema}")]:
        #initial load
        spark.sql(f"""
                  create table if not exists {silver_target_schema}.{silver_target_table} 
                  location "{silver_destination_path}"
                """)
        ### implement data quality
        writedf = df.select([col for col in raw_columns])
        writedf.selectExpr(silver_schema).write.mode("overwrite").option("path",f"{silver_destination_path}").option('overwriteSchema','True').saveAsTable(f"{silver_target_schema}.{silver_target_table}")

    else:
        pk_condition = return_update_and_pf_str(pk_col, "pk")
        update_condition = return_update_and_pf_str(raw_columns,"update")
        insert_col = return_insert_sel_val_str(raw_columns,'select')
        value_col = return_insert_sel_val_str(raw_columns,'value')

        print(f'pk condition: {pk_condition}')
        print(f'update_condition: {update_condition}')
        print(f'insert_col: {insert_col}')
        print(f'value_col: {value_col}')

        if load_type == "DeltaLoad": #DeltaLoad
            spark.sql(
                f"""
                merge into {silver_target_schema}.{silver_target_table} as t using {raw_target_schema}_source_view as s on {pk_condition}
                when matched and s._change_type == "delete" then delete 
                when matched and s._change_type == "update_postimage" then update set {update_condition}
                when not matched and s._change_type == "insert" then insert ({insert_col}) values ({value_col})          
                """
            )
        else:  # FullLoad
            spark.sql(f"""
                      truncate table {silver_target_schema}.{silver_target_table};
                      """)
            print(f" {silver_target_schema}.{silver_target_table} truncated")
            spark.sql(
                f"""
                merge into {silver_target_schema}.{silver_target_table} as t using {raw_target_schema}_source_view as s on {pk_condition}
                when matched and s._change_type == "delete" then delete 
                when matched and s._change_type == "update_postimage" then update set {update_condition}
                when not matched and s._change_type == "insert" then insert ({insert_col}) values ({value_col})          
                """
            )
            print(f'{silver_target_table} FullLoad complete')

 
