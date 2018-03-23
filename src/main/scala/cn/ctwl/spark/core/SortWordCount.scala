package cn.ctwl.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 排序的wordcount程序
 * 
 */
object SortWordCount {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
        .builder()
        .appName("LocalFile")
        .master("local")
        .config("spark.sql.warehouse.dir", "C:\\Users\\CTWLPC\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    val sc = spark.sparkContext
    
    val lines = sc.textFile("C://Users//CTWLPC//Desktop//spark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word  => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)
 
    // 到这里为止，就得到了每个单词出现的次数
		// 但是，问题是，我们的新需求，是要按照每个单词出现次数的顺序，降序排序
		// wordCounts RDD内的元素是什么？应该是这种格式的吧：(hello, 3) (you, 2)
		// 我们需要将RDD转换成(3, hello) (2, you)的这种格式，才能根据单词出现次数进行排序把！
		
		// 进行key-value的反转映射
    val countWords = wordCounts.map(wordCount => (wordCount._2, wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    val sortedWordCounts = sortedCountWords.map(sortedCountWord => (sortedCountWord._2, sortedCountWord._1))
    
    sortedWordCounts.foreach(sortedWordCount => println(
        sortedWordCount._1 + " +++ " + sortedWordCount._2 + " times ."))
  }
  
}