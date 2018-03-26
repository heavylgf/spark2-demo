package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 第37讲-Spark核心编程：共享变量（Broadcast Variable和Accumulator）
 * 累加变量
 */
object AccumulatorVariable {
  
  def main(args: Array[String]): Unit = {
     val spark = SparkSession
        .builder()
        .appName("BroadcastVariable")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    // 创建Accumulator变量
    // 需要调用SparkContext的accumulator()方法
    val sum = sc.accumulator(0)
    val sum1 = sc.accumulator(0)
    
    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    
    // 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
    numbers.foreach { num => sum += num }
    numbers.foreach { num => sum1.add(num) }
     
    println("sum: ==" + sum)
    println("sum1: ==" + sum1)
    
  }
  
}