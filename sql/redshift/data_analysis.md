## Redshift Spectrum による S3 直接読込
Redshift Query Editor  
```sql
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'orders_db'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
```
```sql
SELECT COUNT(*)
FROM spectrum_schema.curated;
```
## COPY による内部テーブルへのロード
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

##
```sql
COPY curated_internal
FROM 's3://<curated-bucket>/orders/curated/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftRole'
FORMAT AS PARQUET;
```
```
SELECT COUNT(*) FROM curated_internal;
```
## データ分析のための集計クエリ作成
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
## Materialized View 作成
```sql
CREATE MATERIALIZED VIEW mv_monthly_country_sales
AS
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales,
  SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_orders
FROM curated_internal
GROUP BY 1,2;
```
```sql
SELECT * 
FROM mv_monthly_country_sales
ORDER BY month, country;
```
## MV 更新
```sql
REFRESH MATERIALIZED VIEW mv_monthly_country_sales;
``` 
## 内部テーブル vs 
```sql
SELECT
  date_trunc('month', order_ts) AS month,
  country,
  COUNT(*) AS orders,
  SUM(amount_with_tax) AS gross_sales
FROM curated_internal
GROUP BY 1,2
ORDER BY 1,2;
```
## 返金率の分析
```sql
SELECT
  month,
  country,
  orders,
  refunded_orders,
  (refunded_orders::decimal(18,4) / NULLIF(orders,0)) AS refund_rate
FROM mv_monthly_country_sales
ORDER BY month, country;
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
## 削除
```sql
DROP MATERIALIZED VIEW IF EXISTS mv_monthly_country_sales;
DROP TABLE IF EXISTS curated_internal;
```
