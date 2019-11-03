package mapreduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class PublicationMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  //mapper to calculate the number of different types of published per year

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("TagInfo.conf")
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  def getPublications(xml: String):String = {
    //reading the tag name for each publication
    val pub = XML.loadString(xml)
    val pubDoc = pub.head.child
    val pubType = pubDoc.head.label
    pubType
  }

  def getYear(xml: String): String = {
    logger.info("Getting Year of publication : " + xml)
    val year = XML.loadString(xml)
    val yearPublished = (year \\ "year").text
    yearPublished
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>" //value holds only one publication record at a time

    val pubInput = getPublications(xmlToProcess)
    val year = getYear(xmlToProcess)

    context.write(new Text(pubInput+","+year), new IntWritable(1))
    logger.info("key="+pubInput+","+year)

  }

}

