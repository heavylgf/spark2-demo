package cn.ctwl.spark.streaming

import org.apache.spark.sql.SparkSession

object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
          .builder()
          .appName("StreamingWordCount")
//          .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
          .getOrCreate()
        
    // 导入隐式转换
    import spark.implicits._
    
    // 创建输入流，拿到的是一个DataFrame类型
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
    
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    
    // 输出
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")  // 输出到命令行
      .start()

    query.awaitTermination()
    
    /**
     * 右键打包
     * run as > run configurations  
     * 
     * vim structured_streaming_wordcount.sh
     * /usr/local/spark/bin/spark-submit \
			--class cn.ctwl.spark.streaming.WordCount \
			--num-executors 3 \
			--driver-memory 1000m \
			--executor-memory 1000m \
			--executor-cores 3 \
			/usr/local/test_spark_app/spark2-demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
     * 
     * 
     */
    
  }
  
}