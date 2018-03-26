package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 取最大的前3个数字
 */
object Top3 {
  
  def main(args: Array[String]): Unit = {
     val spark = SparkSession
     
      .builder()
      .appName("LocalFile")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
      .getOrCreate()
      
     val sc = spark.sparkContext
    
     val lines = sc.textFile("C://Users//CTWLPC//Desktop//top.txt", 1)
     val pairs = lines.map { line => (line.toInt, line) }
     val sortedPairs = pairs.sortByKey(false)
     val sortedNumbers = sortedPairs.map(sortedPair => sortedPair._1)
     
     val top3Number = sortedNumbers.take(3)
     
     for(num <- top3Number){
       println(num)
     }
     
  }
  
}