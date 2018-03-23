package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.collection.CompactBuffer

/**
 * 第34讲-Spark核心编程：transformation操作开发实战
 */
object TransformationOperation {
  
  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
    cogroup()
    
  }
  
  /**
   * map算子案例：将集合中每一个元素都乘以2
   */
  def map(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val numbers = Array(1, 2, 3, 4, 5)
    // 并行化集合，创建初始RDD
    val numberRDD = sc.parallelize(numbers, 1)
    val multipleNumberRDD = numberRDD.map { num => num * 2 }
    // 打印新的RDD
    multipleNumberRDD.foreach { num => println(num) }
    
    
  }
  
  /**
   * filter算子案例：过滤集合中的偶数
   */
  def filter(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 1)
    val evenNumberRDD = numberRDD.filter { num => num % 2 == 0 }
    evenNumberRDD.foreach { num => println(num) }
    
  }
  
  /**
   * flatMap案例：将文本行拆分为多个单词
   */
  def flatMap(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val lineArray = Array("hello you", "hello me", "hello world")
    val lines = sc.parallelize(lineArray, 1)
    val words = lines.flatMap { line => line.split(" ") }
    
    words.foreach { word => println(word) }
    
  }
  
  /**
   * groupByKey案例：按照班级对成绩进行分组
   */
  def groupByKey(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
        Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val groupedScores = scores.groupByKey()
    
    groupedScores.foreach(score => {
      println(score._1)
      score._2.foreach { score => println(score) }
      println("===========")
    })
    
  }
  
  /**
   * reduceByKey案例：统计每个班级的总分
   */
  def reduceByKey(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
        Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val totalScores = scores.reduceByKey(_ + _)
    
    totalScores.foreach(classScore => println(classScore._1 + ": " + classScore._2))
    
    
  }
  
  /**
   * sortByKey案例：按照学生分数进行排序
   */
  def sortByKey(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"), 
        Tuple2(100, "marry"), Tuple2(85, "jack")) 
    val scores = sc.parallelize(scoreList, 1)
    
    val sortedScores = scores.sortByKey()
    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))
    
  }
  
  /**
   * join案例：打印学生成绩
   */
  def join(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val studentList = Array(
        Tuple2(1, "leo"),
        Tuple2(2, "jack"),
        Tuple2(3, "tom"));
    
   val scoreList = Array(
        Tuple2(1, 100),
        Tuple2(2, 90),
        Tuple2(3, 60));
   
   val students = sc.parallelize(studentList, 1)
   val scores = sc.parallelize(scoreList, 1)
   
   val studentScores = students.join(scores)
   
   studentScores.foreach(studentScore => {
     println("student id:" + studentScore._1)
     println("student name:" + studentScore._2._1)
     println("student score:" + studentScore._2._2)
     println("=================")
   })
    
    
  }
  
  /**
   * cogroup案例：打印学生成绩
   */
  def cogroup(){
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val studentList = Array(
        Tuple2(1, "leo"),
        Tuple2(2, "jack"),
        Tuple2(3, "tom"));
    
   val scoreList = Array(
        Tuple2(1, 100),
        Tuple2(2, 90),
        Tuple2(3, 60));
   
   val students = sc.parallelize(studentList, 1)
   val scores = sc.parallelize(scoreList, 1)
   
   val studentScores = students.cogroup(scores)
   
   studentScores.foreach(studentScore => {
     println("student id:" + studentScore._1)
     println("student name:" + studentScore._2._1.toString())
     println("student score:" + studentScore._2._2.toString())
     println("=================")
   })
   
  }
  
  
  
  
  
  
  
  
  
  
  
  
}