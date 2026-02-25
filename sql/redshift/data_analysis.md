## Redshift Spectrum による S3 直接読込
Redshift query editor v2 を開いて Serverless: orders-workgroup を選択  
IAM ロールは ARN で指定する  
```sql
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'orders_db'
IAM_ROLE 'arn:aws:iam::XXX:role/GlueEtlWorkflowStack-RedshiftRoleXXX...'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
```
```sql
SELECT COUNT(*)
FROM spectrum_schema.curated;
```
```
【実行結果】
count
66785
```
## COPY による内部テーブルへのロード
テーブル作成  
```sql
CREATE TABLE curated_internal
(
  order_id varchar(50),
  user_id varchar(50),
  order_ts timestamp,
  amount double precision,
  currency varchar(10),
  country varchar(10),
  status varchar(20),
  tax_rate double precision,
  amount_with_tax double precision,
  is_high_value boolean
)
DISTSTYLE AUTO
SORTKEY(order_ts);
```
テーブルに S3 のデータをコピー  
##
```sql
COPY curated_internal
FROM 's3://glueetlworkflowstack-curatedbucket6a59c97e-emaaucs4wnbt/orders/curated/'
IAM_ROLE 'arn:aws:iam::XXX:role/GlueEtlWorkflowStack-RedshiftRoleXXX...'
FORMAT AS PARQUET;
```
```
【実行結果】
Info:
Load into table 'curated_internal' completed, 66785 record(s) loaded successfully.
Returned rows: 0
Query ID: 206666
Elapsed time: 13.1s
Result set query:
/* RQEV2-zFDIROu0tI */
COPY curated_internal
FROM 's3://glueetlworkflowstack-curatedbucket6a59c97e-emaaucs4wnbt/orders/curated/'
IAM_ROLE 'arn:aws:iam::XXX...'
FORMAT AS PARQUET
```
```
SELECT COUNT(*) FROM curated_internal;
```
```
【実行結果】
count
66785

S3 (Data Lake)
      ↓
Spectrum (外部参照)
      ↓
Redshift COPY
      ↓
Redshift内部ストレージ（DWH）👈ここまで完了
```
## データ分析のための集計クエリ作成
- GROUP BY 1,2 は SELECT 句の1番目と2番目の列で GROUP BY している  
- date_trunc('month', order_ts) は order_ts を「月単位」に切り落とす
- SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) は 'REFUNDED' のみ 1 として集計 (返金件数)
```sql
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales,
  SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_orders
FROM curated_internal
GROUP BY 1,2
ORDER BY 1,2;
```
```
【実行結果】
month contry orders gross_sales refunded_orders
2026-01-01 00:00:00	JP	7543	206344826.74000007	3740	
2026-01-01 00:00:00	US	3750	95516610.27999997	1938	
2026-02-01 00:00:00	JP	6835	188080415.07	3398	
2026-02-01 00:00:00	US	3446	85240948.33000007	1757	
2026-03-01 00:00:00	JP	7684	213102842.24999976	3793	
2026-03-01 00:00:00	US	3827	95196465.04999998	1923	
2026-04-01 00:00:00	JP	7370	205444727.26000023	3787	
2026-04-01 00:00:00	US	3743	93093278.57999998	1881	
2026-05-01 00:00:00	JP	7705	213785421.03000012	3914	
2026-05-01 00:00:00	US	3841	95970182.94000009	1895	
2026-06-01 00:00:00	JP	7394	204814582.2900001	3695	
2026-06-01 00:00:00	US	3647	89282198.17999999	1791	
```
## Materialized View 作成
```sql
CREATE MATERIALIZED VIEW mv_monthly_country_sales AS
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales
FROM curated_internal
GROUP BY 1,2;
```
```
【実行結果】
month country orders gross_sales
2026-02-01 00:00:00	JP	6835	188080415.0699999	
2026-03-01 00:00:00	JP	7684	213102842.24999988	
2026-04-01 00:00:00	US	3743	93093278.57999994	
2026-04-01 00:00:00	JP	7370	205444727.2600003	
2026-02-01 00:00:00	US	3446	85240948.33000006	
2026-05-01 00:00:00	JP	7705	213785421.03000015	
2026-03-01 00:00:00	US	3827	95196465.04999998	
2026-06-01 00:00:00	US	3647	89282198.18	
2026-01-01 00:00:00	JP	7543	206344826.74000013	
2026-01-01 00:00:00	US	3750	95516610.27999997	
2026-05-01 00:00:00	US	3841	95970182.94000007	
2026-06-01 00:00:00	JP	7394	204814582.2900001	

→ Views に mv_monthly_country_sales が作成される
```
## クエリの実行時間比較 (元テーブル vs マテリアライズドビュー)
① 元テーブルから実行  
```sql
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales
FROM curated_internal
GROUP BY 1,2;
```
```
【実行時間】
Query ID: 409220
Elapsed time: 14846 ms
Total rows: 12

month	country	orders	gross_sales
2026-02-01 00:00:00	JP	6835	188080415.0699999	
2026-03-01 00:00:00	JP	7684	213102842.24999988	
2026-04-01 00:00:00	US	3743	93093278.57999994	
2026-04-01 00:00:00	JP	7370	205444727.2600003	
2026-01-01 00:00:00	JP	7543	206344826.74000013	
2026-01-01 00:00:00	US	3750	95516610.27999997	
2026-02-01 00:00:00	US	3446	85240948.33000006	
2026-05-01 00:00:00	JP	7705	213785421.03000015	
2026-05-01 00:00:00	US	3841	95970182.94000007	
2026-06-01 00:00:00	JP	7394	204814582.2900001	
2026-03-01 00:00:00	US	3827	95196465.04999998	
2026-06-01 00:00:00	US	3647	89282198.18	
```
② マテリアライズドビューから実行  
```sql
SELECT *
FROM mv_monthly_country_sales;
```
```
【実行結果】
Query ID: 409257
Elapsed time: 127 ms
Total rows: 12

month	country	orders	gross_sales
2026-02-01 00:00:00	JP	6835	188080415.0699999	
2026-03-01 00:00:00	JP	7684	213102842.24999988	
2026-04-01 00:00:00	US	3743	93093278.57999994	
2026-04-01 00:00:00	JP	7370	205444727.2600003	
2026-02-01 00:00:00	US	3446	85240948.33000006	
2026-05-01 00:00:00	JP	7705	213785421.03000015	
2026-03-01 00:00:00	US	3827	95196465.04999998	
2026-06-01 00:00:00	US	3647	89282198.18	
2026-01-01 00:00:00	JP	7543	206344826.74000013	
2026-01-01 00:00:00	US	3750	95516610.27999997	
2026-05-01 00:00:00	US	3841	95970182.94000007	
2026-06-01 00:00:00	JP	7394	204814582.2900001	
```
## マテリアライズドビューの更新
現時点の MV の値  
```sql
SELECT * FROM mv_monthly_country_sales
WHERE month='2026-06-01' AND country='JP';
```
```
【実行結果】
month	country	orders	gross_sales
2026-06-01 00:00:00	JP	7394	204814582.2900001	
```
追加データを内部テーブルに1行入れる  
```sql
INSERT INTO curated_internal
(order_id,user_id,order_ts,amount,currency,country,status,tax_rate,amount_with_tax,is_high_value)
VALUES
('o-test-1','u-test','2026-06-15 10:00:00',10000,'JPY','JP','COMPLETED',0.1,11000,true);
```
元のテーブルを確認
```sql
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales
FROM curated_internal
WHERE date_trunc('month', order_ts)='2026-06-01' AND country='JP'
GROUP BY 1,2;
```
```
【実行結果】
month	country	orders	gross_sales
2026-06-01 00:00:00	JP	7395	204825582.29000002	
→ orders が1件増加
```
MV の再確認
```sql
SELECT * FROM mv_monthly_country_sales
WHERE month='2026-06-01' AND country='JP';
```
```
【実行結果】
month	country	orders	gross_sales
2026-06-01 00:00:00	JP	7394	204814582.2900001	
→ orders の増加が反映されていない
```
MV のリフレッシュ
```sql
REFRESH MATERIALIZED VIEW mv_monthly_country_sales;
```
MV の再確認
```sql
SELECT * FROM mv_monthly_country_sales
WHERE month='2026-06-01' AND country='JP';
```
```
【実行結果】
month	country	orders	gross_sales
2026-06-01 00:00:00	JP	7395	204825582.2900001	
→ 追加レコードが反映されていることを確認
```
## 高額注文比率の分析
```sql
CREATE MATERIALIZED VIEW mv_monthly_country_kpis
AS
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales,
  SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_orders
FROM curated_internal
GROUP BY 1,2;
```
```
【実行結果】
month	country	orders	gross_sales	high_value_orders
2026-02-01 00:00:00	JP	6835	188080415.06999993	5420	
2026-03-01 00:00:00	JP	7684	213102842.24999976	6184	
2026-04-01 00:00:00	JP	7370	205444727.26000017	5958	
2026-04-01 00:00:00	US	3743	93093278.57999997	2981	
2026-01-01 00:00:00	JP	7543	206344826.7400001	5987	
2026-01-01 00:00:00	US	3750	95516610.27999994	3025	
2026-02-01 00:00:00	US	3446	85240948.33000004	2740	
2026-05-01 00:00:00	JP	7705	213785421.03000012	6161	
2026-03-01 00:00:00	US	3827	95196465.05000001	3071	
2026-06-01 00:00:00	US	3647	89282198.18000004	2891	
2026-05-01 00:00:00	US	3841	95970182.94000001	3094	
2026-06-01 00:00:00	JP	7395	204825582.2900002	5927	
```
## EXPLAIN で実行計画を確認
元のテーブル  
```sql
EXPLAIN
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales
FROM curated_internal
GROUP BY 1,2;
```
```
【実行結果】
QUERY PLAN
XN HashAggregate  (cost=0.24..0.27 rows=6 width=30)	
  ->  XN Seq Scan on mv_tbl__mv_monthly_country_sales__0 derived_table1  (cost=0.00..0.12 rows=12 width=30)	
```
MV  
```sql
EXPLAIN
SELECT *
FROM mv_monthly_country_sales;
```
```
【実行結果】
QUERY PLAN
XN Seq Scan on mv_tbl__mv_monthly_country_sales__0 derived_table1  (cost=0.00..0.12 rows=12 width=30)	
```
## SORTKEY の効果
ソートキー(order_ts) で絞り込み  
```sql
SELECT COUNT(*)
FROM curated_internal
WHERE order_ts BETWEEN '2026-06-01' AND '2026-06-30';
```
```
【実行結果】
Query ID: 612544
Elapsed time: 221 ms
Total rows: 1

count
11042	
```
非ソートキーで絞り込み  
```sql
SELECT COUNT(*)
FROM curated_internal
WHERE country = 'JP';
```
```
【実行結果】
Query ID: 612591
Elapsed time: 5313 ms
Total rows: 1

count
44532
```
## DISTSTYLE の確認
現在は AUTO分散
```sql
SELECT "table", diststyle
FROM svv_table_info
WHERE "table" = 'curated_internal';
```
```
【実行結果】
table	diststyle
curated_internal	AUTO(EVEN)
→ AUTO分散の結果、EVEN分散が選ばれている
```
## Concurrency / Serverless
```
Redshift Serverlessは：
	•	RPU単位で自動スケール
	•	ワークロードに応じてスケールアップ
	•	使ってないときは縮小
```

## 削除
```sql
DROP MATERIALIZED VIEW IF EXISTS mv_monthly_country_sales;
DROP TABLE IF EXISTS curated_internal;
```
