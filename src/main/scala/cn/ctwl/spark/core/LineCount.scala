package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 第33讲-Spark核心编程：操作RDD（transformation和action案例实战）
 * 统计每行出现的次数
 */
object LineCount {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val lines = sc.textFile("C:\\Users\\CTWLPC\\Desktop\\spark.txt", 1)
    val pairs = lines.map { line => (line, 1) }
    val lineCounts = pairs.reduceByKey(_ + _)
    
    
    lineCounts.foreach(lineCount => println(lineCount._1 + " appears " + lineCount._2 + " times."))
        
    
  }
  
}