## 常用git command集合

列出目前local有哪些分支: 
```git branch```

列出目前remote有哪些分支: 
```git branch -r --list```

把遠端的分支抓下來且切換: 
```git switch <分支名稱>```

切換branch：\
```git checkout <分支名稱>```  或  ```git switch <分支名稱>```

建立新的branch且切換過去：\
```git checkout -b <分支名稱>``` 或  ```git switch -c <分支名稱>```

把（當下分支）遠端的資料拉下來: 
```git fetch```

把（當下分支）遠端的資料拉下來且commit: 
```git pull```

把別的分支合併「進來」且commit: 
```git merge <另外一個分支的名稱>```

### 常見疑問
- fetch跟pull的差異: 擷取自 [高見龍gitbook](https://gitbook.tw/interview#)
```
git fetch

假設遠端節點叫做 origin，當執行 git fetch 指令的時候，Git 會比對本機與遠端（在這邊就是 origin）專案的差別，會「下載 origin 上面有但我本機目前還沒有」的內容下來，並且在本地端形成相對應的分支。

不過，fetch 指令只做下載，並不會進行合併。

git pull

pull 指令其實做的事情跟 fetch 是一樣的，差別只在於 fetch 只有把檔案抓下來，但 pull 不只抓下來，還會順便進行合併。也就是說，本質上，git pull 其實就等於 git fetch 加上 merge 指令。
```



### 情境
1. 要測試合併看看，看一下會有什麼問題/差別（且容易復原）
```
git merge --no-commit --no-ff <要merge進來的分支>

#如果要放棄merge
git merge --abort

#如果確定要merge就再去commit
```

2. 用遠端分支的最新版本覆蓋目前的分支 / 還原目前的分支成為該遠端分支的最新版本
```
git reset --hard origin/<分支名稱>
```