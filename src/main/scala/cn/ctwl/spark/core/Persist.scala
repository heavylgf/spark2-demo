package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * RDD持久化
 */
object Persist {
  
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("LocalFile")
    .master("local")
    .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
    .getOrCreate()
    
    val sc = spark.sparkContext
    
    // cache()或者persist()的使用，是有规则的
		// 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
		// 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
		// 而且，会报错，大量的文件会丢失
    val lines = sc.textFile("C:\\Users\\CTWLPC\\Desktop\\spark.txt", 1)
        .persist(StorageLevel.MEMORY_ONLY_2)
        
//    val linell = sc.textFile("C:\\Users\\CTWLPC\\Desktop\\spark.txt", 1)
//    .persist(StorageLevel.MEMORY_ONLY)
    
    val beginTime = System.currentTimeMillis();
    val count = lines.count()
    println(count)
    
    val endTime = System.currentTimeMillis();
    println("cost " + (endTime - beginTime))
    
    
    val beginTime1 = System.currentTimeMillis();
    val count1 = lines.count()
    println(count1)
    val endTime1 = System.currentTimeMillis();
    println("cost 1 " + (endTime1 - beginTime1))
    
  }
  
}