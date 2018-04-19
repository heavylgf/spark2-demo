package cn.ctwl.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object RowNumberWindowFunction {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("RDD2DataFrameProgrammatically")
          .master("local")
          .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
          .getOrCreate()
     
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    // 这里着重说明一下！！！
    // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
    import sqlContext.implicits._
    
    
    
    
  }
}