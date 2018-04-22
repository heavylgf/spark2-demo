//package cn.ctwl.spark.sql
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.types.DataTypes
//import java.util.ArrayList
//import org.apache.spark.sql.Row
//
//object JDBCDataSource {
//  
//  def main(args: Array[String]): Unit = {
//    
//    val spark = SparkSession
//        .builder()
//        .appName("DataFrameCreate")
//        .master("local[2]")
//        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
//        .getOrCreate()
//        
//    val sc = spark.sparkContext
//    
//    // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
////    val hiveContext = new HiveContext(sc)
//    val sqlContext = new SQLContext(sc)
//    
//    // 总结一下
//    // jdbc数据源
//    // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
//    // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
//    // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中
//    
//    
//    // 分别将mysql中两张表的数据加载为DataFrame
//    val option = Map("url" -> "jdbc:mysql://spark1:3306/testdb",
//        "dbtable" -> "student_infos")
//        
//    val studentInfoDF = sqlContext.read.format("jdbc").options(option).load()
//        
////    val studentInfoDF = sqlContext.read.format("jdbc").options( 
////        Map("url" -> "jdbc:mysql://spark1:3306/testdb",
////        "dbtable" -> "student_infos")).load()
//    
//    val studentScoresDF = sqlContext.read.format("jdbc").options(
//        Map("url" -> "jdbc:mysql://spark1:3306/testdb",
//          "dbtable" -> "student_scores")).load()
//    
//    // 将两个DataFrame转换为JavaPairRDD，执行join操作
//    val studentsRDD = studentInfoDF.rdd.map { 
//        studentInfoRDD => (studentInfoRDD.get(1), studentInfoRDD.get(2)) }
//        .join(studentScoresDF.rdd.map { 
//        studentScoresRDD => (studentScoresRDD.get(1), studentScoresRDD.get(2)) })
//    
//    // 将PairRDD转换为RDD<Row>
//    val studentRowsRDD = studentsRDD.map(studentRDD => Row(studentRDD._1, studentRDD._2._1, studentRDD._2._2))
////      val studentRowsRDD = studentsRDD.map(studentRDD => Tuple2(studentRDD._1, studentRDD._2._1, studentRDD._2._2))
//
//    // 过滤出分数大于80分的数据
//    val filterRdd = studentRowsRDD.filter{ row => row.get(2).toString().toInt > 80 }
//        
//    // 尝试打印
//    filterRdd.collect()
//    filterRdd.foreach( rdd => println(rdd.get(1) + ":" + rdd.get(2) + ":" + rdd.get(3)))
//    
//    // 转换为DataFrame
//    val schema = StructType(Array(
//        StructField("name", StringType, true),
//        StructField("age", IntegerType, true),
//        StructField("score", IntegerType, true)))
//  
////    val structType = StructType(Array(
////      StructField("id", IntegerType, true),
////      StructField("name", StringType, true),
////      StructField("age", IntegerType, true)))  
//    
////    // 第三步，进行RDD到DataFrame的转换  
//    val studentDF = sqlContext.createDataFrame(filterRdd, schema)
////    
////    
////    val structType = DataTypes.createStructType(structFields)
////
////       val personDf = sqlContext.createDataFrame(rowsRDD, structType)
////       personDf.registerTempTable("persons")
////    
////    
//    
//    
//    
//  }
//  
//}