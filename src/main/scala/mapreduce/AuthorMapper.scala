package mapreduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class AuthorMapper extends Mapper[LongWritable, Text, Text, FloatWritable] {
  //mapper to calculate authorship scores

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

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {

    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>" //value holds only one publication record at a time


    val authorList = getAuthors(xmlToProcess)

    logger.info("Calculating the Score for the following authors: " + authorList)

    var score: Float = 1f / (authorList.size).floatValue()

    for (author <- authorList.reverse) { //iterate over the list in reverse order of authors to calculate the relative authorship scores
      if(author.equalsIgnoreCase(authorList.head)){
        score = score
      } else {
        score = (3f*score)/4f
      }
      context.write(new Text(author), new FloatWritable(score))
      logger.info("key="+author +"\t value="+score)

      score = score + 1f/(4f*authorList.size)
    }

  }


}
