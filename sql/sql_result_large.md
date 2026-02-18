## テーブル確認
```sql
SHOW TABLES;
```

## 内容確認
```sql
SELECT *
FROM curated
LIMIT 10;
```

## パーティション指定なし
```sql
SELECT COUNT(*)
FROM curated;
```
```
【実行結果】
キュー内の時間: 110 ms  
実行時間: 1.218 sec
```

## パーティション・プルーニング
```sql
SELECT COUNT(*)
FROM curated
WHERE year = '2026'
  AND month = '2';
```
```
【実行結果】  
キュー内の時間: 124 ms  
実行時間: 654 ms
```

## Parquetの列指向
```sql
SELECT COUNT(*)
FROM curated
WHERE year = '2026' AND month = '2' AND is_high_value = true;
```
```
【実行結果】  
キュー内の時間: 116 ms
実行時間: 725 ms
```

```sql
SELECT order_id, amount
FROM curated
WHERE year = '2026' AND month = '2'
LIMIT 100;
```
```
【実行結果】
キュー内の時間: 113 ms  
実行時間: 510 ms
```

## データスキャン量の分析
```sql
SELECT sum(amount)
FROM curated
WHERE year='2026' AND month='2';
```
```
【実行結果】
キュー内の時間: 112 ms  
実行時間: 580 ms  
スキャンしたデータ: 69.89 KB
```

```sql
SELECT sum(amount_with_tax)
FROM curated
WHERE year='2026' AND month='2';
```
```
【実行結果】
キュー内の時間: 101 ms
実行時間: 842 ms
スキャンしたデータ: 70.11 KB
```

```sql
SELECT count(*)
FROM curated
WHERE year='2026' AND month='2'
  AND country = 'JP'
  AND status = 'COMPLETED'
  AND is_high_value = true;
```
```
【実行結果】
キュー内の時間: 101 ms  
実行時間: 1.27 sec  
スキャンしたデータ: 31.62 KB
```

```sql
SELECT *
FROM curated
WHERE year='2026' AND month='2'
```
```
【実行結果】
キュー内の時間: 109 ms  
実行時間: 1.372 sec  
スキャンしたデータ: 320.23 KB
```
