// WordCount.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    // Example input data
    val inputText = List(
      "Hello world",
      "Hello Spark",
      "Apache Spark example",
      "WordCount program"
    )

    // Parallelize the input data
    val textRDD = sc.parallelize(inputText)

    // Perform WordCount
    val wordCounts = textRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Print the word counts
    wordCounts.collect().foreach(println)

    sc.stop()
  }
}
