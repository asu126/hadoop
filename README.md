### 打包
```
mvn clean package
cd target
hadoop jar hadoop-tset-1.0-SNAPSHOT.jar WordCount
// hadoop fs -rm -r hdfs://localhost:9000/user/su/output-008
```

### I/O 操作
- 序列化：Writable 接口
- 定制Writable 接口
- 基于文件的数据接口
  - SequenceFile(可作为小文件容器)

### 参考文档
- [1](https://blog.csdn.net/qq_39327985/article/details/82991659)
- [2-缓存](https://blog.csdn.net/fhx007/article/details/45491767)
- [3-缓存](https://blog.csdn.net/xiaolang85/article/details/11782539)
- [4-Hadoop客户端提交作业时java.lang.ClassNotFoundException](https://blog.csdn.net/yaoxtao/article/details/64921060)
- [MRunit](https://cwiki.apache.org/confluence/display/MRUNIT/Testing+Word+Count)
