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
キュー内の時間: 105 ms  
実行時間: 677 ms
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
キュー内の時間: 113 ms  
実行時間: 587 ms
→ パーティション指定なしよりも実行時間が短くなる
```
## Parquetの列指向
```sql
SELECT COUNT(*)
FROM curated
WHERE year = '2026' AND month = '2' AND is_high_value = true;
```
```
【実行結果】  
キュー内の時間: 102 ms  
実行時間: 495 ms
```
```sql
SELECT order_id, amount
FROM curated
WHERE year = '2026' AND month = '2'
LIMIT 100;
```
```
【実行結果】
キュー内の時間: 102 ms  
実行時間: 772 ms
```
