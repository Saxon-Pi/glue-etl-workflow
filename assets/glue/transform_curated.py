import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, upper, trim, when, round

args = getResolvedOptions(sys.argv, ["STAGING_BUCKET","STAGING_PREFIX","CURATED_BUCKET","CURATED_PREFIX"])
in_path = f"s3://{args['STAGING_BUCKET']}/{args['STAGING_PREFIX']}"
out_path = f"s3://{args['CURATED_BUCKET']}/{args['CURATED_PREFIX']}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet(in_path) # 全パーティションをまとめて読み込む

# データ変換 (正規化/フィルタ/重複排除/型変換/列作成)
df2 = (
    df
    .withColumn("country", upper(trim(col("country")))) # country を大文字化 + 空白削除
    .withColumn("status", upper(trim(col("status"))))   # status が CANCELLED 以外を残す
    .filter(col("status") != "CANCELLED")
    .dropDuplicates(["order_id"])                       # order_id の重複を削除 (順序保証は要ロジック)
    .withColumn("amount", col("amount").cast("double")) # amount の型を double に
    # 列追加 (tax_rate): country=JP なら 0.1 (それ以外は 0.0)
    .withColumn("tax_rate", when(col("country") == "JP", 0.10).otherwise(0.0))
    # 列追加 (amount_with_tax): amount に tax_rate を反映させた額
    .withColumn("amount_with_tax", round(col("amount") * (1 + col("tax_rate")), 2))
    # 列追加 (is_high_value): amount が 10000 以上なら True
    .withColumn("is_high_value", when(col("amount") >= 10000, True).otherwise(False))
)

# パーティショニング & Parquet 保存
(df2.write.mode("overwrite")
    .format("parquet")
    .partitionBy("year","month","day")
    .save(out_path))
