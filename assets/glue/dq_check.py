import sys, json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


args = getResolvedOptions(sys.argv, ["RAW_BUCKET","RAW_PREFIX","ALERT_TOPIC_ARN"])
raw_path = f"s3://{args['RAW_BUCKET']}/{args['RAW_PREFIX']}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 の CSV を読み込んで Spark DataFrame を作成
df = (
    spark.read
    .option("header", "true")       # 一行目をヘッダとして扱う
    .option("inferSchema", "true")  # 型推論あり
    .csv(raw_path)                  # CSV パス
)

errors = []

"""
Glue (Spark) ジョブによるデータ品質チェック
    1. order_id の null チェック
    2. order_id の unique チェック
    3. amount の符号チェック (マイナスNG)
    4. status の値チェック (COMPLETED, CANCELLED, REFUNDED 以外はNG)
"""

if df.filter("order_id is null").count() > 0:
    errors.append("order_id has null")

# 重複している order_id の個数
dup = (
    df.groupBy("order_id").count() # order_idごとの件数
        .filter("count > 1")       # 2件以上のorder_idだけ残す（= 重複してるID）
        .count()                   # # 重複してるIDが何種類あるか
)
if dup > 0:
    errors.append(f"order_id has duplicates: {dup} keys")

neg = df.filter("amount < 0").count()
if neg > 0:
    errors.append(f"amount has negative rows: {neg}")

bad_status = df.filter("status not in ('COMPLETED','CANCELLED','REFUNDED')").count()
if bad_status > 0:
    errors.append(f"status has invalid rows: {bad_status}")

# エラーハンドリング (SNS通知)
if errors:
    msg = {"result": "FAILED", "errors": errors, "raw_path": raw_path}
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=args["ALERT_TOPIC_ARN"],
        Subject="Glue DQ FAILED: orders",
        Message=json.dumps(msg, ensure_ascii=False)
    )
    print(json.dumps(msg, ensure_ascii=False))
    sys.exit(1)

print(json.dumps({"result":"PASSED","raw_path":raw_path}, ensure_ascii=False))
