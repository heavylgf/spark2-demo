package cn.ctwl.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

/**
 * DataFrame的常用操作
 */
object DataFrameOperation {
  
  def main(args: Array[String]): Unit = {
     val spark = SparkSession
        .builder()
        .appName("DataFrameCreate")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
     val sc = spark.sparkContext
     val sqlContext = new SQLContext(sc)
     
     // 创建出来的DataFrame完全可以理解为一张表
//		val df = sqlContext.read().json("hdfs://spark1:9000/students.json");  
     val df = sqlContext.read.json("C:\\Users\\CTWLPC\\Desktop\\employee.json")
     
     // 打印DataFrame中所有的数据（select * from ...）
     df.show()
     
     // 打印DataFrame的元数据（Schema）
     df.printSchema()
     
     // 查询某列所有的数据
     df.select("name").show()
     
     // 查询某几列所有的数据，并对列进行计算
     df.select(df("name"), df("age") + 1).show()
     
     // 根据某一列的值进行过滤
     df.filter(df("age") > 18).show()
     
     // 根据某一列进行分组，然后进行聚合
     df.groupBy("age").count().show()
   
  }
  
}