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
    //     - DQ チェックで Fail したら SNS メール通知
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
    // #4. データカタログ作成
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

    // Glue ジョブ #3 (データ変換)
    const trJob = new glue.CfnJob(this, "TransformJob", {
      name: "orders-transform-curated",
      role: glueRole.roleArn,
      glueVersion: "4.0",
      numberOfWorkers: 2,
      workerType: "G.1X",
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: trScript,
      },
      defaultArguments: {
        "--job-language": "python",
        "--STAGING_BUCKET": stagingBucket.bucketName,
        "--STAGING_PREFIX": "orders/parquet/",
        "--CURATED_BUCKET": curatedBucket.bucketName, // 変換ファイル格納バケット
        "--CURATED_PREFIX": "orders/curated/",        // prefix
        "--enable-continuous-cloudwatch-log": "true",
      },
      executionProperty: { maxConcurrentRuns: 1 },
    });

    // Glue Database
    const glueDatabase = new glue.CfnDatabase(this, "OrdersDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: "orders_db",
      },
    });

    // クローラ作成
    const crawler = new glue.CfnCrawler(this, "OrdersCrawler", {
      name: "orders-curated-crawler",
      role: glueRole.roleArn,
      databaseName: glueDatabase.ref,
      targets: {
        s3Targets: [
          {
            path: `s3://${curatedBucket.bucketName}/orders/curated/`, // データ変換後のバケット
          },
        ],
      },
    });

    // Glue ワークフロー
    const workflow = new glue.CfnWorkflow(this, "OrdersWorkflow", {
      name: "orders-etl-workflow",
    });

    // Step 1: DQ チェック
    const t1 = new glue.CfnTrigger(this, "TriggerStartDQ", {
      name: "t-start-dq",
      type: "ON_DEMAND",
      workflowName: workflow.name,
      actions: [{ jobName: dqJob.name! }],
    });

    // Step 2: Parquet 変換
    // ** Glue コンソールの Data Integration and ETL > Triggers から t-after-dq を Activate trigger する必要あり **
    // -> startOnCreation: true に最初からしておけば問題ない、はず
    const t2 = new glue.CfnTrigger(this, "TriggerAfterDQ", {
      name: "t-after-dq",
      type: "CONDITIONAL",   // predicate の条件が成立したら発火する
      workflowName: workflow.name,
      startOnCreation: true, // 自動で Activate
      predicate: {
        logical: "AND",      // conditions が 1つでも AND にしておく (エラー回避)
        conditions: [
        // dqJob の実行結果（state）が SUCCEEDED と等しい場合に、このトリガーを発火する
          { 
            jobName: dqJob.name!,
            state: "SUCCEEDED",
            logicalOperator: "EQUALS",
          }
        ],
      },
      actions: [{ jobName: c2pJob.name! }],
    });

    // Step 3: データ変換
    const t3 = new glue.CfnTrigger(this, "TriggerAfterC2P", {
      name: "t-after-c2p",
      type: "CONDITIONAL",
      workflowName: workflow.name,
      startOnCreation: true,
      predicate: {
        logical: "AND",
        conditions: [
          {
            jobName: c2pJob.name!,
            state: "SUCCEEDED",
            logicalOperator: "EQUALS",
          },
        ],
      },
      actions: [{ jobName: trJob.name! }],
    });

    // Step 4: クローラ実行
    const t4 = new glue.CfnTrigger(this, "TriggerAfterTransform", {
      name: "t-after-transform",
      type: "CONDITIONAL",
      workflowName: workflow.name,
      startOnCreation: true,
      predicate: {
        logical: "AND",
        conditions: [
          {
            jobName: trJob.name!,
            state: "SUCCEEDED",
            logicalOperator: "EQUALS",
          },
        ],
      },
      actions: [
        {
          crawlerName: crawler.name!,
        },
      ],
    });

    // 依存関係
    t1.addDependency(workflow);
    t1.addDependency(dqJob);

    t2.addDependency(workflow);
    t2.addDependency(dqJob);
    t2.addDependency(c2pJob);

    t3.addDependency(workflow);
    t3.addDependency(c2pJob);
    t3.addDependency(trJob);

    t4.addDependency(workflow);
    t4.addDependency(trJob);
    t4.addDependency(crawler);

    // Outputs
    new cdk.CfnOutput(this, "RawBucketName", { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, "WorkflowName", { value: workflow.name! });
    new cdk.CfnOutput(this, "StartTriggerName", { value: t1.name! });
    new cdk.CfnOutput(this, "AlertTopicArn", { value: alertTopic.topicArn });
  }
}
