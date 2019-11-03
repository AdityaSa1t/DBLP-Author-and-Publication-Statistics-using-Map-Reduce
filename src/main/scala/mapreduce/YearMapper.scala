package mapreduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class YearMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  //mapper to calculate number of publications in a year

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("TagInfo.conf")
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  def getYear(xml: String): String = {
    val year = XML.loadString(xml)
    val yearPublished = (year \\ "year").text
    yearPublished
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>" //value holds only one publication record at a time

    val pubYear = getYear(xmlToProcess)
    logger.info("Generating number of publications in a year for individual years")
    context.write(new Text(pubYear), new IntWritable(1))

  }


}
