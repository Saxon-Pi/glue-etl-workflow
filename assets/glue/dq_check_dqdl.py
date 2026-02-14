import sys, json
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Glue Data Quality
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ["RAW_BUCKET","RAW_PREFIX", "ALERT_TOPIC_ARN",])
raw_path = f"s3://{args['RAW_BUCKET']}/{args['RAW_PREFIX']}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 の CSV を読み込んで Spark DataFrame を作成
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

# Spark DF -> DynamicFrame（Data Quality用）
dyf = DynamicFrame.fromDF(df, glueContext, "orders_raw")

"""
データ品質定義言語 (DQDL) ルールセット
    1. order_id の null チェック
    2. order_id の unique チェック
    3. amount の符号チェック (マイナスNG)
    4. status の値チェック (COMPLETED, CANCELLED, REFUNDED 以外はNG)
"""

ruleset = """
Rules = [
  IsComplete "order_id",
  IsUnique "order_id",
  ColumnValues "amount" >= 0,
  ColumnValues "status" in ["COMPLETED","CANCELLED","REFUNDED"]
]
"""

# データ品質チェックの実行 (EvaluateDataQuality)
dq_result = EvaluateDataQuality().process_rows(
    frame=dyf,
    ruleset=ruleset,
    publishing_options={
        # CloudWatchメトリクスを使用する場合は有効
        # "dataQualityEvaluationContext": "orders-dq",
        # "enableDataQualityCloudWatchMetrics": True,
        # "enableDataQualityResultsPublishing": True,
    }
)

# dq_result は DynamicFrameCollection として出力される
# rules_outcomes: ルール単位の集計結果 (ルール違反の件数など)
rules_outcomes = dq_result.select("rulesOutcomes")
# row_outcomes: 行単位の評価結果 (各レコードがどのルールに違反したかなど)
row_outcomes = dq_result.select("rowLevelOutcomes")

# rulesOutcomes を Spark DF にして集計（失敗ルールがあるか）
rules_df = rules_outcomes.toDF()

print("=== rulesOutcomes schema ===")
rules_df.printSchema()
print("=== rulesOutcomes sample ===")
rules_df.show(truncate=False)

# rulesOutcomes の列名が環境でブレることがあるので、どの列が存在するかで失敗ルールの抽出法を切り替える
# ->「失敗が 1つでもあれば落とす」仕様とする
failed_rules = []
cols = set(rules_df.columns) # DataFrame に存在する列名一覧

if "Outcome" in cols and "Rule" in cols:
    # Outcome 列を使用するパターン: Outcome 列が Passed でない (=Failed) なら失敗
    failed = rules_df.filter(rules_df["Outcome"] != "Passed").select("Rule", "Outcome").collect()
    failed_rules = [{"rule": r["Rule"], "outcome": r["Outcome"]} for r in failed]

elif "Passed" in cols and "Rule" in cols:
    # Passed 列を使用するパターン: Passed 列が false なら失敗
    failed = rules_df.filter(rules_df["Passed"] == False).select("Rule", "Passed").collect()
    failed_rules = [{"rule": r["Rule"], "passed": r["Passed"]} for r in failed]

else:
    # 上記パターンに当てはまらない場合: 失敗とする (コードを修正して吸収するように)
    print("rulesOutcomes columns are unexpected:", rules_df.columns)
    failed_rules = [{"rule": "UNKNOWN_SCHEMA", "detail": "rulesOutcomes schema unexpected"}]

# エラーハンドリング (SNS通知)
if failed_rules:
    msg = {
        "result": "FAILED",
        "raw_path": raw_path,
        "failed_rules": failed_rules,
    }
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=args["ALERT_TOPIC_ARN"],
        Subject="Glue DQ FAILED: orders (DQDL)",
        Message=json.dumps(msg, ensure_ascii=False)
    )
    print(json.dumps(msg, ensure_ascii=False))
    sys.exit(1)

print(json.dumps({"result": "PASSED", "raw_path": raw_path}, ensure_ascii=False))
