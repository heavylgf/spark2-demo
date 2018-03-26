package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 分组取top3
 * 对每个班级内的学生成绩，取出前3名
 * @author Administrator
 *
 */
object GroupTop3 {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("LocalFile")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
      .getOrCreate()
      
    val sc = spark.sparkContext
     
    val lines = sc.textFile("C://Users//CTWLPC//Desktop//score.txt", 1)
    val lines1 = sc.textFile("C://Users//CTWLPC//Desktop//score.txt", 1)
     
    // 每行切割开来，映射成一个RDD
    val pairs = lines.map { line => (line.split(" ")(0), line.split(" ")(1)) }
    
    // 进行分组，这样就拿到了所有班级的成绩
    val groupedPairs = pairs.groupByKey()
    
    // 对每个组取出前三个
    val top3Score = groupedPairs.map(groupedPair => {
      val className = groupedPair._1
      val top3 = groupedPair._2.toArray.sortWith(_>_).take(3)
      val top31 = groupedPair._2.take(3)
      (className, top3)
    })
    
    top3Score.foreach(item => {
      val className = item._1
      println("班级：" + className)
      item._2.foreach { score => println("前三名的分数：" + score) }
    })
    
    
    /**
    * 1、通过map算子形成映射(class,score)
    * 2、通过groupByKey算子针对班级Key进行分组
    * 3、通过map算子对分组之后的数据取前3名，核心代码：val top3=m._2.toArray.sortWith(_>_).take(3)
    * 4、通过foreach算子遍历输出
    */
    lines1.map { m => {
      val info = m.split(" ")
      (info(0), info(1).toInt)
    }}.groupByKey()
    .map(m => {
      val className = m._1
      val top3 = m._2.toArray.sortWith(_>_).take(3)
      (className, top3)
    }).foreach(item => {
      val className = item._1
      println("班级：" + className)
      item._2.foreach { m => println("分数是：" + m) }
    }) 
    
     
  }
  
}