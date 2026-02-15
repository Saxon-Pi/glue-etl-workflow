import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as glue from "aws-cdk-lib/aws-glue";

export class GlueEtlWorkflowStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ----------------------- ワークフロー構成 -----------------------
    //
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
    //
    // -------------------------------------------------------------

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

    // SNS トピック (メール通知用)
    const alertTopic = new sns.Topic(this, "DqAlertTopic");

    // Glue ロール
    const glueRole = new iam.Role(this, "GlueJobRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
      ],
    });
    rawBucket.grantRead(glueRole);
    stagingBucket.grantReadWrite(glueRole);
    curatedBucket.grantReadWrite(glueRole);
    alertTopic.grantPublish(glueRole);

    // Glue ジョブスクリプトを S3 に格納
    new s3deploy.BucketDeployment(this, "DeployGlueScripts", {
      sources: [s3deploy.Source.asset("assets/glue")],
      destinationBucket: rawBucket, // 一旦 rawBucket に格納
      destinationKeyPrefix: "glue-scripts",
    });

    // DQ チェック / CSV -> Parquet 変換 / データ変換
    const dqScript = `s3://${rawBucket.bucketName}/glue-scripts/dq_check_dqdl.py`;
    const c2pScript = `s3://${rawBucket.bucketName}/glue-scripts/csv_to_parquet.py`;
    const trScript = `s3://${rawBucket.bucketName}/glue-scripts/transform_curated.py`;

    // Glue ジョブ #1 (DQチェック)
    const dqJob = new glue.CfnJob(this, "DqJob", {
      name: "orders-dq-check",
      role: glueRole.roleArn,
      glueVersion: "4.0",
      numberOfWorkers: 2,
      workerType: "G.1X",
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: dqScript,
      },
      defaultArguments: {
        "--job-language": "python",
        "--RAW_BUCKET": rawBucket.bucketName,         // 生データ格納バケット
        "--RAW_PREFIX": "orders/raw/",                // prefix
        "--ALERT_TOPIC_ARN": alertTopic.topicArn,     // SNS トピック
        "--enable-continuous-cloudwatch-log": "true", // ログ
      },
      executionProperty: { maxConcurrentRuns: 1 },
    });

    // Glue ジョブ #2 (CSV->Parquet変換)
    const c2pJob = new glue.CfnJob(this, "CsvToParquetJob", {
      name: "orders-csv-to-parquet",
      role: glueRole.roleArn,
      glueVersion: "4.0",
      numberOfWorkers: 2,
      workerType: "G.1X",
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: c2pScript
      },
      defaultArguments: {
        "--job-language": "python",
        "--RAW_BUCKET": rawBucket.bucketName,
        "--RAW_PREFIX": "orders/raw/",
        "--STAGING_BUCKET": stagingBucket.bucketName, // 変換ファイル格納バケット
        "--STAGING_PREFIX": "orders/parquet/",        // prefix
        "--enable-continuous-cloudwatch-log": "true",
      },
      executionProperty: { maxConcurrentRuns: 1 },
    });


  }
}
