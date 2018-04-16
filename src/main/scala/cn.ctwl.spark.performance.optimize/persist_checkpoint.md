2018-04-05
## 第5讲-Spark性能优化：对多次使用的RDD进行持久化或Checkpoint
#### 对多次使用的RDD进行持久化或Checkpoint

如果程序中，对某一个RDD，基于它进行了多次transformation或者action操作。那么就非常有必要对其进行持久化操作，以避免对一个RDD反复进行计算。

此外，如果要保证在RDD的持久化数据可能丢失的情况下，还要保证高性能，那么可以对RDD进行Checkpoint操作。



