package cn.ctwl.spark

import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCount {
  
  def main(args: Array[String]): Unit = {
    
    // 创建一个 SparkSession
    val spark = SparkSession
        .builder()
        .appName("StructuredNetworkWordCount")
        .getOrCreate()
        
    import spark.implicits._
    
    // 创建一个输入流
    val lines = spark.readStream
        .format("socket")   // 设置数据流的格式
        .option("host", "localhost")
        .option("port", "9999")
        .load()
    
    // 转换成dataset类型
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    
    val query = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()
        
    query.awaitTermination()
        
  }
  
  /**
   * 打包步骤：
   * 项目右键 > Run As > Run Configrations > maven build
   * 将打包的jar包上传到master机器上 
   * 编写struct_streaming.sh
   * /usr/local/spark/bin/spark-submit \
		--class cn.ctwl.spark.StructuredNetworkWordCount \
		--master spark://spark2upgrade01:7077 \
		--num-executors 1 \
		--driver-memory 500m \
		--executor-memory 500m \
		--executor-cores 1 \
		/usr/local/test_spark_app/spark2-upgrade-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
   */
  
  
  
}