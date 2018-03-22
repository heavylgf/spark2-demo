package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession


/**
 * 第29讲-Spark核心编程：使用Java、Scala和spark-shell开发wordcount程序
 */
object WordCount {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("WordCount")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    
    val lines = sc.textFile("C:\\Users\\CTWLPC\\Desktop\\spark.txt", 1)
    
//    val employee = spark.read.json("C:\\Users\\CTWLPC\\Desktop\\employee.json")
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)
    
    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
    
  }
  
}