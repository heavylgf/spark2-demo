package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 二次排序
 * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 * @author Administrator
 *
 * 
 */
object SecondSort {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
    .builder()
    .appName("LocalFile")
    .master("local")
    .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
    .getOrCreate()
    
    val sc = spark.sparkContext
    
    val lines = sc.textFile("C:\\Users\\CTWLPC\\Desktop\\sort.txt", 1)
    val pairs = lines.map { line => (
        new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line )}
    
    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(sortedPair => sortedPair._2)
    
    sortedLines.foreach { sortedLine => println(sortedLine) }
    
  }
  
}