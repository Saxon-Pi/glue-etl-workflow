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
    # CloudWatchメトリクスを使用する場合は有効にする
    publishing_options={
        # "dataQualityEvaluationContext": "orders-dq",
        # "enableDataQualityCloudWatchMetrics": True,
        # "enableDataQualityResultsPublishing": True,
    }
)
print("DQ_RESULT_KEYS=", dq_result.keys())

# dq_result は DynamicFrameCollection として出力される
# rules_outcomes: ルール単位の集計結果 (ルール違反の件数など)
rules_outcomes = dq_result.select("ruleOutcomes")
# row_outcomes: 行単位の評価結果 (各レコードがどのルールに違反したかなど)
row_outcomes   = dq_result.select("rowLevelOutcomes")

# rulesOutcomes を Spark DF にして集計（失敗ルールがあるか）
rules_df = rules_outcomes.toDF()

print("=== ruleOutcomes schema ===")
rules_df.printSchema()
print("=== ruleOutcomes sample ===")
rules_df.show(truncate=False)

# rulesOutcomes の列名が環境でブレることがあるので、どの列が存在するかで失敗ルールの抽出法を切り替える
# ->「失敗が 1つでもあれば落とす」仕様とする
failed_rules = []
cols = set(rules_df.columns) # DataFrame に存在する列名一覧

# Outcome 列を使用するパターン: Outcome 列が Passed でない (=Failed) なら失敗
if "Outcome" in cols and "Rule" in cols:
    # FailureReason / EvaluatedMetrics を通知に含める
    sel_cols = ["Rule", "Outcome"]
    if "FailureReason" in cols:
        sel_cols.append("FailureReason")
    if "EvaluatedMetrics" in cols:
        sel_cols.append("EvaluatedMetrics")
    if "EvaluatedRule" in cols:
        sel_cols.append("EvaluatedRule")

    failed = (rules_df
              .filter(rules_df["Outcome"] != "Passed")
              .select(*sel_cols)
              .collect())

    failed_rules = []
    for r in failed:
        item = {
            "rule": r["Rule"],
            "outcome": r["Outcome"],
        }
        if "FailureReason" in sel_cols:
            item["reason"] = r["FailureReason"]
        if "EvaluatedMetrics" in sel_cols:
            item["metrics"] = dict(r["EvaluatedMetrics"]) if r["EvaluatedMetrics"] is not None else None
        if "EvaluatedRule" in sel_cols:
            item["evaluated_rule"] = r["EvaluatedRule"]
        failed_rules.append(item)

# Passed 列を使用するパターン: Passed 列が false なら失敗
elif "Passed" in cols and "Rule" in cols:
    sel_cols = ["Rule", "Passed"]
    if "FailureReason" in cols:
        sel_cols.append("FailureReason")
    if "EvaluatedMetrics" in cols:
        sel_cols.append("EvaluatedMetrics")
    if "EvaluatedRule" in cols:
        sel_cols.append("EvaluatedRule")

    failed = (rules_df
              .filter(rules_df["Passed"] == False)
              .select(*sel_cols)
              .collect())

    failed_rules = []
    for r in failed:
        item = {"rule": r["Rule"], "passed": r["Passed"]}
        if "FailureReason" in sel_cols:
            item["reason"] = r["FailureReason"]
        if "EvaluatedMetrics" in sel_cols:
            item["metrics"] = dict(r["EvaluatedMetrics"]) if r["EvaluatedMetrics"] is not None else None
        if "EvaluatedRule" in sel_cols:
            item["evaluated_rule"] = r["EvaluatedRule"]
        failed_rules.append(item)

# 上記パターンに当てはまらない場合: 失敗とする (コードを修正して吸収するように)
else:
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
