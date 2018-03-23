package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * action操作实战
 * 
 */
object ActionOperation {
  
  def main(args: Array[String]): Unit = {
//    reduce()
    countByKey()
    
  }
  
  def reduce() {
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)  
    
    val sum = numbers.reduce(_ + _)
    println(sum)
    
  }
  
  def collect() {
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)  
    
    // 不用foreach action操作，在远程集群上遍历rdd中的元素
		// 而使用collect操作，将分布在远程集群上的doubleNumbers RDD的数据拉取到本地
		// 这种方式，一般不建议使用，因为如果rdd中的数据量比较大的话，比如超过1万条
			// 那么性能会比较差，因为要从远程走大量的网络传输，将数据获取到本地
			// 此外，除了性能差，还可能在rdd中数据量特别大的情况下，发生oom异常，内存溢出
		// 因此，通常，还是推荐使用foreach action操作，来对最终的rdd元素进行处理
    val doubleNumbers = numbers.map { num => num * 2 }
    val doubleNumberArray = doubleNumbers.collect()
    
    for(num <- doubleNumberArray) {
      println(num)
    }
  }
    
    def count(){
      val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)  
    
    // 对rdd使用count操作，统计它有多少个元素
    val count = numbers.count()
    
    println(count)
      
    }
    
    def take() {
      val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
      val sc = spark.sparkContext
      
      val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val numbers = sc.parallelize(numberArray, 1)  
      
      // 对rdd使用count操作，统计它有多少个元素
  		// take操作，与collect类似，也是从远程集群上，获取rdd的数据
  		// 但是collect是获取rdd的所有数据，take只是获取前n个数据
      val top2Numbers = numbers.take(2)
      
      for(num <- top2Numbers){
        println(num)
      }
      
    }
    
    def saveAsTextFile(){
      val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
      val sc = spark.sparkContext
      
      val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val numbers = sc.parallelize(numberArray, 1)  
      
      val doubleNumbers = numbers.map { num => num * 2 }
      
      // 直接将rdd中的数据，保存在HFDS文件中
  		// 但是要注意，我们这里只能指定文件夹，也就是目录
  		// 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
      doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt")
      
    }
    
    // 对每个key对应的value进行count操作
    def countByKey(){
      val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
      val sc = spark.sparkContext
      
      val studentList = Array(Tuple2("class1", "leo"), Tuple2("class2", "jack"),
          Tuple2("class1", "tom"), Tuple2("class2", "jen"), Tuple2("class2", "marry"))   
      val students = sc.parallelize(studentList, 1)  
      
      val studentCounts = students.countByKey()
      
      println(studentCounts)
      
      // Map(class1 -> 2, class2 -> 3)
    
    } 

}