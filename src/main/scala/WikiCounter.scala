package main

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.util.StringUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.xml.XML

object WikiCounter {
  def main(args: Array[String]) {
    val conf    = new SparkConf().setAppName("Word Counter")
    val sc      = new SparkContext(conf)
    val jobConf = new JobConf()

    jobConf.set("stream.recordreader.class",
      "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page")
    jobConf.set("stream.recordreader.end", "</page>")
    FileInputFormat.addInputPaths(jobConf,
      "file:///Users/di/Books/enwiki-20160204-pages-articles1.xml-p000000010p000030302")

    val wikiDocuments = sc.hadoopRDD(jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text], classOf[Text])
        .map(_._1.toString)

    val rawWikiPages = wikiDocuments.map(wikiString => {
      val wikiXml = XML.loadString(wikiString)
      (wikiXml \ "revision" \ "text").text
    })

    val tokenizedWikiData = rawWikiPages
      .map(_.replaceAll("[.|,|'|\"|?|)|(|_|0-9]", " ").trim)
      .flatMap(_.split("\\W+"))
      .filter(_.length > 2)

    val sortedByLength = tokenizedWikiData.distinct
      .sortBy(_.length, ascending = false)
      .sample(withReplacement = false, fraction = 0.1)

    sortedByLength.saveAsTextFile("/tmp/wiki_pages")
  }
}
