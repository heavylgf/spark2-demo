package cn.ctwl.spark

import org.apache.spark.sql.SparkSession

/**
 * 初步入门SparkSession、Dataframe、Dataset的基本开发
 */
object SaprkSQLDemo {
  
  // 定义一个case class
  // 会用dataset，通常都会通过case class来定义dataset的数据结构，自定义类型其实就是一种强类型，也就是typed
  case class Person(name: String, age: Long)
  
  def main(args: Array[String]): Unit = {
    
    // 构造SparkSession，基于build()来构造
    val spark = SparkSession  
        .builder()   // 用到了java里面的构造器设计模式
        .appName("SaprkSQLDemo")
        .master("local")
        // spark.sql.warehouse.dir，必须设置
        // 这是Spark SQL 2.0里面一个重要的变化，需要设置spark sql的元数据仓库的目录
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
//         启用hive支持
//        .enableHiveSupport()
        .getOrCreate()
        
        import spark.implicits._
        
        // 读取json文件，构造一个untyped弱类型的dataframe
        // dataframe就相当于Dataset[Row]
        val df = spark.read.json("C:\\Users\\CTWLPC\\Desktop\\people.json")
//        df.show()  // 打印数据
//        df.printSchema()   // 打印元数据
//        df.select("name").show()  // select操作，典型的弱类型，untyped操作
//        df.select($"name", $"age" + 1).show() // 使用表达式，scala的语法，要用$符号作为前缀
//        
//        df.filter($"age" > 21).show()   // filter操作+表达式的一个应用
//        df.groupBy("age").count().show()   // groupBy分组，再聚合
        
        // 基于dataframe创建临时视图
//        df.createOrReplaceTempView("people")
//        // 使用SparkSession的sql()函数就可以执行sql语句，默认是针对创建的临时视图的
//        val sqlDF = spark.sql("select * from people")
//        sqlDF.show()
        
        
        // 首先在main()外定义一个case class
        
        // 直接基于jvm object来构造dataset
        val caseClassDS = Seq(Person("Andy", 32)).toDS()  // Seq()相当于用一个集合去封装
        caseClassDS.show()
        
        // 基于原始数据类型构造dataset
        val primitiveDS = Seq(1, 2, 3).toDS()
        primitiveDS.map { _ + 1 }.show()
        
        // 基于已有的结构化数据文件，构造dataset
        // spark.read.json()，首先获取到的是dataframe，其次使用as[Person]之后，就可以将一个dataframe转换为一个dataset
        val peopleDS  = spark.read.json("C:\\Users\\CTWLPC\\Desktop\\people.json").as[Person]
        peopleDS.show()
       
         
  }
 
  
}