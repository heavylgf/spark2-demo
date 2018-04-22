//package cn.ctwl.spark.sql
//
//import java.io.File
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.sql.types.StringType
//
//object ReadHive {
//  
//  def main(args: Array[String]): Unit = {
//    
//    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
//    val format = "yyyy-MM-dd'T'HH:mm:ssz"
//    val spark = SparkSession
//      .builder()
//      .master("local[2]")
//      .appName("ReadHive")
//      .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
//
//    import spark.sql
//    import  spark.implicits
//    
//    spark.sql("use default")
//    spark.sql("select student.Sname,course.Cname,sc.Grade from student join sc " +
//      "on student.Sno=sc.Sno " +
//      "join course on sc.cno=course.cno")
//      .show()
//    
//    val schema = StructType(List(
//        StructField("id", IntegerType, true),
//        StructField("name", StringType, true),
//        StructField("gender", StringType, true),
//        StructField("age", IntegerType, true)))
//    val DF = spark.sql("select student.Sname,course.Cname,sc.Grade from student join sc " +
//      "on student.Sno=sc.Sno " +
//      "join course on sc.cno=course.cno")
//    // 直接将DF注册为一张临时表
//    DF.registerTempTable("tempTable")
//    
//    val teenagerDF = spark.sql("")
//    val teenagerRDD = teenagerDF.rdd
//    
////    // 执行SQL语句
////    val teenagerDF = sqlContext.sql("select * from students where age<=18")
////    
////    // 转换成普通的RDD
////    val teenagerRDD = teenagerDF.rdd
//    
//    
//    
//    spark.stop()
//    
//  }
//}