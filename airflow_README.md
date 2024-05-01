先跑CLI
```
docker-compose up airflow-init
docker-compose up -d 
```

到localhost:8000
帳密都是airflow

假設跑起來8080沒有通
要進到webserver裡面
``` docker exec -it tir101_group2-airflow-webserver-1 bash ```
檢查一下資料夾權限 ```ls -l```
如果有資料夾沒有寫入權限 就要把它改掉 ```chmod 777 <資料夾名稱>```
然後再跑一次 ```airflow webserver```



理論上crawler_with_pymongo就可以直接執行

如果要跑crawler_with_mongohook要做環境設定。到Admin > Connection 然後依照附圖把資料填進去，存檔後就可以跑這個DAG

![airflow_connection_setup](images/airflow_connection_screenshot.png)
