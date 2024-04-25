## 環境設定

- 建議大家裝autopep8套件 (施老師還有推很多extension我們再慢慢選用)

### VENV setup
暫時先用venv，之後可以再換到別的\
venv的目的是切出一個專門給這個專案使用的python enviornment\
優點是不會有版本衝突，有需要也可以快速重設

```
# make一個venv 位置在.venv的資料夾裡
python -m venv .venv  

#Mac 切換到venv裡面
source .venv/bin/activate

#Powershell 切換到venv裡面
.venv\Scripts\Activate.ps1

#把大家都需要用的套件裝起來
pip install -r requirements.txt
```

### DB docker setup
這裡加入了老師沒有特別提到的Volume\
概念跟airflow的 -v 目的一樣，是把資料存到local電腦裡
但是這個存的位置給Docker volume去管理

MySQL Docker
```
# 建立一個docker管理的空間（資料夾）
docker volume create mysql_volume

# 用repo裡面的mysql.Dockerfile建立起image
docker build -f mysql.Dockerfile -t custom_mysql .

# 跑建立好的image + port & volume binding
docker run -p 55000:3306 -v mysql_volume:/var/lib/mysql -d custom_mysql
```
MongoDB Docker（指令幾乎都跟上面一樣）

```
# MongoDB Dockerfile
docker volume create mongodb_volume
docker build -f mongoDB.Dockerfile -t custom_mongo .
docker run -p 27000:3306 -v mongodb_volume:/data/db -d custom_mongo
```
如果資料庫被完壞了，就把container跟volume刪掉\
然後重新跑volume create跟run就好了（不用重build）

#### 用Studio 3T連到MongoDB
```
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
