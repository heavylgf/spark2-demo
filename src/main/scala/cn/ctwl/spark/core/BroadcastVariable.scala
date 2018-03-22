package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
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
    val factor = 3
    val factorBroadcast = sc.broadcast(factor)
    
    val numbersArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numbersArray, 1)
    
    val multipleNumbers = numbers.map { num => num * factorBroadcast.value }
    
    multipleNumbers.foreach { num => println(num) }
    
  }
  
}