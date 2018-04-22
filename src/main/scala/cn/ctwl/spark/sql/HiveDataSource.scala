//package cn.ctwl.spark.sql
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.DataFrameReader
//
//object HiveDataSource {
//  
//  
//  def main(args: Array[String]): Unit = {
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
//    val hiveContext = new HiveContext(sc)
//    
//    // 第一个功能，使用HiveContext的sql()方法，可以执行Hive中能够执行的HiveQL语句
//    
//    // 判断是否存在student_infos表，如果存在则删除
//    hiveContext.sql("DROP TABLE IF EXISTS student_infos")
//    
//    // 判断student_infos表是否不存在，如果不存在，则创建该表
//    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)")
//    
//    // 将学生基本信息数据导入student_infos表
//    hiveContext.sql("LOAD DATA LOCAL INPATH '/user/root/resources/student_infos.txt' INTO TABLE student_infos")
//    
//    // 用同样的方式给student_scores导入数据
//    hiveContext.sql("DROP TABLE IF EXISTS student_scores")
//    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)")
//    hiveContext.sql("LOAD DATA LOCAL INPATH '/user/root/resources/student_score.txt' INTO TABLE student_scores")
//
//    // 第二个功能，执行sql还可以返回DataFrame，用于查询
//    val goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
//	      + "FROM student_infos si "
//	      + "JOIN student_scores ss" 
//	      + "on si.name = ss.name "
//	      + "WHERE ss.score >= 80")
//    
//    // 第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
//		// 将DataFrame中的数据保存到hive表中
//		
//		// 接着将DataFrame中的数据保存到good_student_infos表中
//    hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
////    goodStudentsDF.saveAsTable("good_student_infos");  
////    
////    // 第四个功能，可以用table()方法，针对hive表，直接创建DataFrame
////		
////		// 然后针对good_student_infos表，直接创建DataFrame
////		Row[] goodStudentRows = hiveContext.table("good_student_infos").collect(); // 这里返回的是一行一行的数据
////		for(Row goodStudentRow : goodStudentRows) {
////			System.out.println(goodStudentRow);  
////		}
////		
////		sc.close();
//    
//  }
//  
//  
//}