package cn.ctwl.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.scalacheck.Prop.False
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._


/**
 * 用户访问日志可以统计uv，用户购买日志可以统计销售额
 */
object DailySale {
  
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
    
    // 说明一下，业务的特点
    // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
    // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了
    
    // 模拟数据
    val userSaleLog = Array("2015-10-01,55.05,1122",
        "2015-10-01,23.15,1133",
        "2015-10-01,15.20,",
        "2015-10-02,56.05,1144",
        "2015-10-02,78.87,1155",
        "2015-10-02,113.02,1123")
        
    // 并行化RDD
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)
    
    
    // 进行有效销售日志的过滤
	  // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD，转换为元素为Row的RDD
    val filteredUserSaleLogRDD = userSaleLogRDD
        .filter { log => if (log.split(",").length == 3) true else false }
    
    val userSaleLogRowRDD = filteredUserSaleLogRDD
        .map { log => Row(log.split(",")(0), log.split(",")(1).toDouble) }
    
    // 构造元数据
    val structType = StructType(
        Array(StructField("date", StringType, true),
              StructField("sale_amount", DoubleType, true)))
    
    // 使用SQLContext创建DataFrame
    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)
    
    // 开始进行每日销售额的统计
    userSaleLogDF.groupBy("date")
        .agg('date, sum('sale_amount))
        .collect()
        .foreach { row => println(row(1), row(2)) }
    
//    // 开始进行每日销售额的统计
//    userSaleLogDF.groupBy("date")
//        .agg('date, sum('sale_amount))
//        .map { row => Row(row(1), row(2)) }
//        .collect()
//        .foreach(println)  
  }
    
}

