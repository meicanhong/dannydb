# DannyDB
Danny 学习实现 kv 存储引擎的项目

## Bitcask

### 2023-03-30 

实现第一版本 Bitcask， 完成了读取、写入、删除、Compaction 和数据持久化等操作。
但还是有很多功能未实现，比如：
- 并发读写
- 数据压缩
- 数据文件拆分
- 自动 Compaction

性能测试:
- 测试写入 500w 条数据，总共写入 690MB 数据，耗时 17022 ms，平均每秒写入 29.4w 条数据
- 从 500w 条数据中随机读取 100w 条数据，总共读取 140MB 数据，耗时 3832 ms，平均每秒读取 26.1w 条数据

学习资料:
- https://www.cnblogs.com/meicanhong/p/17234415.html

## Sorted Strings Table
need to do

学习资料:
- https://soulmachine.gitbooks.io/system-design/content/cn/key-value-store.html