package mapreduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class PublicationReducer extends Reducer[Text, IntWritable, Text, Text] {
  //Reducer class to accumulate the number of publication types presented in a year

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {

    val sum = values.asScala.foldLeft(0)(_ + _.get)
    logger.info("Publication type and year:"+ key +"\t total:"+sum)
    context.write(new Text(key.toString), new Text(sum.toString))

  }
}
