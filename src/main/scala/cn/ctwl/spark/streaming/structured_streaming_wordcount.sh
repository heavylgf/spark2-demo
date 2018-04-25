/usr/local/spark/bin/spark-submit \
--class cn.ctwl.spark.streaming.WordCount \
--num-executors 3 \
--driver-memory 1000m \
--executor-memory 1000m \
--executor-cores 3 \
/usr/local/test_spark_app/spark2-demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar \