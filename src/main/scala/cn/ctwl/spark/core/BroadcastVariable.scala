package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 第37讲-Spark核心编程：共享变量（Broadcast Variable和Accumulator）
 * 共享变量
 */
object BroadcastVariable {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
        .builder()
        .appName("BroadcastVariable")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
    // 获取的返回结果是Broadcast<T>类型
    val factor = 3
    // 定义共享变量
    val factorBroadcast = sc.broadcast(factor)
    
    val numbersArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numbersArray, 1)
    
    // 使用共享变量时，调用其value()方法，即可获取其内部封装的值
    val multipleNumbers = numbers.map { num => num * factorBroadcast.value }
    
    multipleNumbers.foreach { num => println(num) }
    
  }
  
}