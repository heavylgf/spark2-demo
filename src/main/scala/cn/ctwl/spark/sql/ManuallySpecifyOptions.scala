package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * @author Administrator
 */
object ManuallySpecifyOptions {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
        .builder()
        .appName("DataFrameCreate")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    val peopleDF = sqlContext.read.format("json").load("hdfs://spark1:9000/people.json")
    peopleDF.select("name").write.format("parquet").save("hdfs://spark1:9000/peopleName_scala") 
    
    peopleDF.write.mode(SaveMode.Append).save("hdfs://spark1:9000/peopleName_scala")  
//    
//    df.show()   
//    
//    
//    
//    val conf = new SparkConf()
//        .setAppName("ManuallySpecifyOptions")  
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//  
//    val peopleDF = sqlContext.read.format("json").load("hdfs://spark1:9000/people.json")
//    peopleDF.select("name").write.format("parquet").save("hdfs://spark1:9000/peopleName_scala")   
//    
//    peopleDF.write.save("hdfs://spark1:9000/peopleName_scala", "", SaveMode.Append)   
  }
  
}