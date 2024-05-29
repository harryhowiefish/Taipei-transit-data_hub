# Taipei-transit-data_hub

用資料工程和大數據對於台北市大眾運輸工具的深度探勘和分析

## 架構圖
![project architecture](docs/images/project_architecture.png)

## 資料集
#### 蒐集資料期間: 
- 站點狀態: 2024/4/16~2024/5/23
- 借車的歷史資料: 2021/4~2024/3
#### 蒐集資料頻率:
- 站點狀態: 每10分鐘 (約1300站）
- 借車的歷史資料: 每個月（*非固定更新）
#### 資料量:
- 站點狀態: 約600萬筆 （200MB)
- 借車的歷史資料: 約8100萬筆 (9GB)

## setup

### run Airflow
For more detail, please refer to the README in the airflow folder.
```
cd airflow
docker-compose up airflow-init
docker-compose up -d
```

### GCP setup

## Contributor 
In alphabetical order:
- Andy
- Harry: @harryhowiefish
- Laura
- Taylor
