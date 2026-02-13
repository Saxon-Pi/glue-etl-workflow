import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";

export class GlueEtlWorkflowStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // -------------------- ワークフロー構成 --------------------
    // #1. Data Quality チェック (DQDL ルールセットで品質判定)
    //     - 入力: Raw CSV
    //     - 出力: CloudWatch Logs + S3 に品質結果 JOIN
    // #2. CSV -> Parquet 変換ジョブ
    //     - 入力: Raw CSV
    //     - 出力: Staging Parquet (partition: year=YYYY/month=MM/day=DD)
    // #3. データ変換ジョブ
    //     - 入力: Staging Parquet
    //     - 出力: Curated Parquet (partition 維持)
    //             - 型変換（order_ts->timestamp、amount->decimal）
    //             - 重複排除 (order_id で dropDuplicate)
    //             - 正規化 (countryを大文字化、空白削除、statusの標準化)
    //             - 派生列追加 (amount_with_tax、order_date、is_high_value)
    //             - 不要行排除 (status != 'CANCELLED')
    // #4. 通知
    //     - DQ チェックで Fail したら SNS メール通知

    // S3 バケット (Raw/Staging/Curated)
    const rawBucket = new s3.Bucket(this, "RawBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        { transitions: [{ storageClass: s3.StorageClass.GLACIER, transitionAfter: cdk.Duration.days(30) }], expiration: cdk.Duration.days(90) },
      ],
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const stagingBucket = new s3.Bucket(this, "StagingBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [{ expiration: cdk.Duration.days(14) }],
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const curatedBucket = new s3.Bucket(this, "CuratedBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });


  }
}
