package main

import org.apache.spark.{SparkContext, SparkConf}


object WordCounter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Counter")
    val sc   = new SparkContext(conf)
    val textFile = sc.textFile("/Users/di/Books/Anna_Korenina.txt")
    val sortedCounts = textFile.flatMap(_.split("""\s+"""))
                          .map(_.replaceAll("""[,\.]+""", ""))
                          .filter(_.length > 4)
                          .map(_.toLowerCase)
                          .map((_, 1))
                          .reduceByKey((acc, newVal) => acc + newVal)
                          .sortBy(_._2, false)
    sortedCounts.saveAsTextFile("/tmp/Anna_Korenina_res")
  }
}