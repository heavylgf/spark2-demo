package cn.ctwl.spark

import org.apache.spark.sql.SparkSession

/**
 * 基础操作
 * 
 * 持久化：cache、persist
 * 创建临时视图：createTempView、createOrReplaceTempView
 * 获取执行计划：explain
 * 查看schema：printSchema
 * 写数据到外部存储：write
 * dataset与dataframe互相转换：as、toDF
 * 
 */
object BasicOperation {
  
//  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)
  case class Employee(name: String, age: Long, depID: Long, gender: String, salary: Long)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("BasicOperation")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    import spark.implicits._
    val employee = spark.read.json("C:\\Users\\CTWLPC\\Desktop\\employee.json")
    // 持久化，在rdd部分仔细讲解过，我们这里就不展开讲了
    // 持久化，如果要对一个dataset重复计算两次的话，那么建议先对这个dataset进行持久化再进行操作，避免重复计算
//    employee.cache()
//    println(employee.count())
//    println(employee.count())
//    
//    // 创建临时视图，主要是为了，可以直接对数据执行sql语句
//    employee.createOrReplaceTempView("employee")
//    spark.sql("select * from employee where age > 30").show()
//    
//    // 获取spark sql的执行计划
//    // dataframe/dataset，比如执行了一个sql语句获取的dataframe，实际上内部包含一个logical plan，逻辑执行计划
//    // 设计执行的时候，首先会通过底层的catalyst optimizer，生成物理执行计划，比如说会做一些优化，比如push filter
//    // 还会通过whole-stage code generation技术去自动化生成代码，提升执行性能
//    spark.sql("select * from employee where age > 30").explain()
//    
//    employee.printSchema()
//    
//    val employeeWithAgeGreaterThen30DF = spark.sql("select * from employee where age > 30")
//    // 注意：这里保存在本地可能会出错，但是在之前操作保存到hdfs是成功的
//    employeeWithAgeGreaterThen30DF.write.json("C:\\Users\\Administrator\\Desktop\\employeeWithAgeGreaterThen30DF.json")
    
    // dataframe 转换成dataset
    val employeeDS = employee.as[Employee]
    employeeDS.show()
    employeeDS.printSchema()
    
    // 将dataSet 转换为 dataframe
    val employeeDF = employeeDS.toDF()
    
    
  }    
  
}