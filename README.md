## 環境設定

- 大家用的python是哪個版本? 3.12
- 建議大家裝autopep8套件 (施老師還有推很多extension我們再慢慢選用)


```
# 暫時先用venv，之後可以再換到別的
python -m venv .venv
source .venv\Scripts\activate
pip install -r requirements.txt
```

DB docker setup
```
# mysql Dockerfile
docker build -f mysql.Dockerfile -t custom_mysql .
docker run -p 55000:3306 -d custom_mysql

# MongoDB Dockerfile
docker build -f mongoDB.Dockerfile -t custom_mongo .
docker run -p 27000:3306 -d custom_mongo

Studio 3T
<Server>
Server: localhost
Port: 27000
<Authentication>
User name: root
password: password
Authentication DB: admin

```




## git best practice
### repo structure
- main有包含unit test，確定正常的版本
- develop 
```
├── main (protected)
├── develop (protected)
    ├── feature_xxx_name
    ├── fix_xxx_name
    └── unit_test_name
```
### 上傳檔案注意事項（超重要）
如果有csv跟json檔超過10MB，請不要commit到git。請依照下面流程操作：
- 把檔案名稱放進gitignore（或是小心不要stage進commit）
- 將檔案放google drive或是別的雲端
- 把連結放在這個readme下面的External file listing

### commit message 規定 (暫定)
- 中文敘述為主(可以寫越細越好)
- 名字 / 功能簡述 / 日期 /n 功能細節
```
# commit message example

Harry / 資料庫DDL / 04_15
- user資料表
- SKU資料表
- sales資料表
```

## PR (pull request)

- 目的
    - 由其他人檢查程式碼（code review）
    - 可以跑自動化測試流程（github action）
- PR message 規定
    - 列出修改的檔案
    - 列出建議其他人幫忙測試的細項 
   <br>(e.x.試跑爬蟲、測試轉檔)</br>
- 操作方式
    - 當你的branch完成的時候到github去選擇pull request。標題不用改，寫上PR message並選擇一個組員幫你做code review。
    - code review完成後在remote端把你的branch刪掉，local要不要留都可以，但就不要再上傳了。
- code review工作
    - 測試PR request列出的測試項目（must）
    - 檢視改動檔案清單有沒有合理 (optional)
    - 程式碼建議（optional）
    - 審核後，完成PR（把原branch刪除）
    - 完成PR後在群組內告知大家，讓大家去把develop merge進自己的branch

## External file listing
