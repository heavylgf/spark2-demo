package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode


/**
 * @author Administrator
 */
object GenericLoadSave {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("GenericLoadSave")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    val usersDF = sqlContext.read.load("hdfs://spark1:9000/users.parquet") // 通用的load加载数据
    usersDF.write.save("hdfs://spark1:9000/namesAndFavColors_scala")  // save操作，数据保存在本地中
    
    
    
//    DataFrame peopleDF = sqlContext.read().format("json")
//				.load("hdfs://spark1:9000/people.json"); 
//		peopleDF.save("hdfs://spark1:9000/people_savemode_test", "json", SaveMode.Append);
    
  }
  
}