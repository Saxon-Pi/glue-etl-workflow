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

```

## Parquetの列指向
```sql
SELECT COUNT(*)
FROM curated
WHERE year = '2026' AND month = '2' AND is_high_value = true;
```
```
【実行結果】  

```

```sql
SELECT order_id, amount
FROM curated
WHERE year = '2026' AND month = '2'
LIMIT 100;
```
```
【実行結果】

```
