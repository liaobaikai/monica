
# 预检查
monica precheck --basedir C:/Users/BK-liao/Documents --input-file C:\Users\BK-liao\monica\123.xlsx --manifest-file C:\Users\BK-liao\monica\manifest.json

# 备份
monica backup -h192.168.6.251 -pdsgdata@000 -uroot -P3306  --basedir C:/Users/BK-liao/Documents --input-file C:\Users\BK-liao\monica\123.xlsx --manifest-file C:\Users\BK-liao\monica\manifest.json

# 批量升级
monica patch -h192.168.6.251 -pdsgdata@000 -uroot -P3306 --basedir C:/Users/BK-liao/Documents --input-file C:\Users\BK-liao\monica\123.xlsx --manifest-file C:\Users\BK-liao\monica\manifest.json

# 查看本地备份目录
monica lsinventory --basedir C:/Users/BK-liao/Documents --input-file C:\Users\BK-liao\monica\123.xlsx --manifest-file C:\Users\BK-liao\monica\manifest.json -w1

## 回退
monica rollback -h192.168.6.251 -pdsgdata@000 -uroot -P3306 --basedir C:/Users/BK-liao/Documents --input-file C:\Users\BK-liao\monica\123.xlsx --manifest-file C:\Users\BK-liao\monica\manifest.json