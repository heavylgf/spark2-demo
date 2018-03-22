package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

object AccumulatorVariable {
  
  def main(args: Array[String]): Unit = {
     val spark = SparkSession
        .builder()
        .appName("BroadcastVariable")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    val sum = sc.accumulator(0)
    
    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    
    numbers.foreach { num => sum += num }
     
    println(sum)
    
  }
  
}