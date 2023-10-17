import os
os.environ["PYSPARK_PYTHON"] = "/usr/local/share/python/python3.11/bin/python"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pyspark
from pyspark.sql import SparkSession

jarpaths = ",".join([os.path.join("/usr/local/spark/jars", j) for j in os.listdir("/usr/local/spark/jars/")])

packages = ",".join(["io.delta:delta-core_2.12:2.1.1",
                     "io.delta:delta-storage:2.1.1",
                     "org.apache.hadoop:hadoop-client:3.3.1",
                     "org.apache.hadoop:hadoop-aws:3.3.1",
                     "com.amazonaws:aws-java-sdk-bundle:1.12.428",
                     "org.apache.spark:spark-sql_2.12:3.3.1",
                     "org.apache.spark:spark-avro_2.12:3.3.1",
                     "org.apache.spark:spark-hive_2.12:3.3.1",
                     "org.apache.spark:spark-core_2.12:3.3.1",
                     "com.databricks:spark-xml_2.12:0.14.0",
                     "ca.uhn.hapi.fhir:hapi-fhir-structures-r4:5.1.0",
                     "com.microsoft.aad:adal4j:0.0.2",
                     "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0",
                     "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8",
                     "org.postgresql:postgresql:42.2.23",
                     "org.slf4j:slf4j-log4j12:2.0.6",
                    ])


s3accessKey = os.getenv("S3ACCESSKEY")
s3secretKey = os.getenv("S3SECRETKEY")
s3Endpoint = os.getenv("S3ENDPOINT")
MAX_MEMORY = "8g"
conf = [
    ("spark.sql.legacy.timeParserPolicy", "CORRECTED"),
    ("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"),
    ("fs.s3a.access.key", s3accessKey),
    ("fs.s3a.secret.key", s3secretKey),
    ("spark.hadoop.fs.s3a.endpoint", s3Endpoint),
    ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
    ("spark.hadoop.fs.s3a.path.style.access", "true"),
    ("spark.hadoop.fs.s3a.connection.ssl.enabled", "true"),
    ("spark.jars", jarpaths),
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
    ("spark.delta.merge.repartitionBeforeWrite", "true"),
    ("spark.executor.memory", MAX_MEMORY),
    ("spark.driver.memory", MAX_MEMORY),
    ("spark.sql.autoBroadcastJoinThreshold", "-1"),

]
#In case of memory emergency, break glass:
#("spark.sql.autoBroadcastJoinThreshold"   "-1")
config = pyspark.SparkConf().setAll(conf)
spark = SparkSession.builder.config(conf=config).getOrCreate()
spark.sparkContext.stop()

spark = SparkSession.builder.config(conf=config).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import logging
logger = logging.getLogger()
logger.setLevel(logging.CRITICAL)



import pandas as pd
pd.options.display.max_rows = 200
pd.options.display.max_columns = 500
pd.set_option('display.max_colwidth', None)






import pyunic as pu
