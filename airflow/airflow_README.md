## 基礎環境設定
- 複製一份.env.sample並改名為.env
- 更改裡面的內容
  - 確保GCP的路徑正確
  - 若是測試階段請補上在BIGQUERY_PREFIX補上名字 e.x. Harry_
  - 若未提供discord webhook，則會有某些DAGs會有error (e.x. bike_realtime_to_gcs)

## Docker compose 操作
切換路徑至airflow(放置docker-compose.yml的資料夾)
```
cd airflow
```
起airflow docker compose\
第一次執行要跑airflow-init（會把postgres設定跑過一次）\
後續執行只要跑第二行即可
```
docker-compose up airflow-init
docker-compose up -d 
```

關閉airflow
```
docker-compose down
```

## 操作Airflow UI
到 [localhost:8000](localhost:8000)\
帳密都是airflow，進去後應該就可以看到各個DAGs

## 其他環境設定/操作
### Email (這部分設定完要重啟docker compose)
如果要讓email可以發送
需要設定把airflow.cfg.example複製一份，改名為airflow.cfg並更改以下設定

smtp_host = smtp.gmail.com
smtp_user = <你的email>
smtp_password = <密碼>
smtp_port = 587
smtp_mail_from = <你的email>

密碼請不要用你個人的密碼，請設定app passwords
https://support.google.com/mail/answer/185833?hl=en



## 問題解決
### 1. UI一直跑不出來
假設跑起來8080沒有通，要進到webserver裡面\
``` docker exec -it tir101_group2-airflow-webserver-1 bash ```\
檢查一下資料夾權限 ```ls -l```\
如果有資料夾沒有寫入權限\
就要把它改掉 ```chmod 777 <資料夾名稱>```\
然後再跑一次 ```airflow webserver```