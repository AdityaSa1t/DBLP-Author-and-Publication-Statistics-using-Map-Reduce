package mapreduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class StatsMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  //mapper to calculate various statistics

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("TagInfo.conf")
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  def getAuthors(xml: String): List[String] = {
    val author = XML.loadString(xml)
    val authors = (author \\ "author").map(author => author.text.toLowerCase.trim).toList
    //extracting all the values under <author> tags
    authors
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>" //value holds only one publication record at a time

    val authorList = getAuthors(xmlToProcess)
    logger.info("Number of Authors " + authorList)
    for (author <- authorList) {
      context.write(new Text(author), new IntWritable(authorList.length-1))
    }

  }


}
