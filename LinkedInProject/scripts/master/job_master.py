import pymongo
import json
import os
import numpy as np
import pandas as pd
from linkedInutility import get_db_connection


import pymongo
import logging
import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import when,col
from pyspark.sql import SQLContext

from pyspark.sql.functions import *
def job_master_data(cfg):
  logging.info("Creating Database connection for master")
 #db_connection = get_db_connection(cfg)
 #dblist=db_connection.list_database_names()
  conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster(
   "local").setAppName("My First Spark Job").setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
  sc = SparkContext(conf=conf)
  sqlC = SQLContext(sc)
  mongo_ip="mongodb://localhost:27017/LinkedInJob."
  print(mongo_ip)
  master=sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip+"Staging_raw_data").load()
  master.createOrReplaceTempView("data")
  # job_id(pk)
  # cmp_id
  # job_post_id
  # job_title
  # job_type
  # job_function
  # job_level
  # job_country
  # job_location
  # job_apply_link
  # job_date_posted
  # job_date_created
  # job_user_created
  from pyspark.sql.functions import lit, StringType

  df =sqlC.sql("SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS job_id,job_id as job_post_id,company as job_cmp_name,title as job_title,location as job_country,place as job_place, seniority_level as job_seniority_level,job_function,link as job_apply_link,date as job_date_posted,employment_type as job_type FROM data")
  #df = df.withColumn('job_seniority_level', lit("Others").cast(StringType()))
  #df = df.withColumn('cmp_ceo_name', F.lit(None).cast(StringType()))
  # df = df.withColumn('cmp_head_office', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_current_openings', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_date_created', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_user_created', F.lit(None).cast('string'))
  #df=df.withColumn('job_seniority_level', when(job_seniority_level ==" ","Others"))
  #df = df.withColumn("job_seniority_level",when(col("job_seniority_level").isin('not_set', 'n/a', 'N/A', 'userid_not_set'),"Others").otherwise(col("job_seniority_level")))
  from pyspark.sql import functions as f
  df = df.withColumn('job_seniority_level', f.expr("coalesce(job_seniority_level, 'Others')"))
  df = df.withColumn('job_type', f.expr("coalesce(job_type, 'Others')"))
  df = df.withColumn('job_function', f.expr("coalesce(job_seniority_level, 'Others')"))
  from pyspark.sql.functions import datediff, col
  df=df.withColumn("current_date", f.current_date())
  df=df.withColumn("job_total_days", datediff(f.current_date(), col("job_date_posted")))
  df = df.withColumn('job_active_flag', when(col('job_total_days') >= 90, 'N').otherwise('Y'))
  print(df.head(100))
  db_connection = get_db_connection(cfg)
  dblist = db_connection.list_database_names()
  if "LinkedInJob" in dblist:
    mydb = db_connection["LinkedInJob"]
    db_cm = mydb["job_master"]
  # data_json = json.loads(df.toJSON().collect())

  results = df.toJSON().map(lambda j: json.loads(j)).collect()
  print(results)
  db_cm.insert_many(results)
  sc.stop()
  #mycol = mydb["Staging"]
  logging.info("The data is inserted into the database")

