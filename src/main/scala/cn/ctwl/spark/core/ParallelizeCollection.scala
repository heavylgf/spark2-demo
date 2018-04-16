package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 并行化集合创建RDD
 * 
 */
object ParallelizeCollection {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 1)
    
    val sum = numberRDD.reduce(_ + _)
    println("累加的和是：" + sum)
    
    
  }
  
}