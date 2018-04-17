package cn.ctwl.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

/**
 * 使用json文件创建DataFrame
 * 
 */
object DataFrameCreate {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("DataFrameCreate")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json("C:\\Users\\CTWLPC\\Desktop\\employee.json")
    
    df.show()   
    
  }
  
  
  
}