import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col ,date_format

args = getResolvedOptions(sys.argv, ["RAW_BUCKET","RAW_PREFIX","STAGING_BUCKET","STAGING_PREFIX"])
raw_path = f"s3://{args['RAW_BUCKET']}/{args['RAW_PREFIX']}"
out_path = f"s3://{args['STAGING_BUCKET']}/{args['STAGING_PREFIX']}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spark DataFrame
df = (
    spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv(raw_path))

# order_ts から年・月・日を取り出して S3 パーティション用の列を作成
df = df.withColumn("year", date_format(col("order_ts"), "yyyy")) \
       .withColumn("month", date_format(col("order_ts"), "MM")) \
       .withColumn("day", date_format(col("order_ts"), "dd"))

# パーティショニング & Parquet 保存
(df.write.mode("overwrite")
   .format("parquet")
   .partitionBy("year","month","day")
   .save(out_path))
