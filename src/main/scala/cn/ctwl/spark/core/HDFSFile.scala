package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**"
 * 第32讲-Spark核心编程：创建RDD（HDFS文件）
 */
object HDFSFile {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    
    val lines = sc.textFile("hdfs://spark1:9000/spark.txt", 1)
    val count = lines.map { line => line.length() }.reduce(_ + _)
    
    println("files count is :" + count)
    
  }
}