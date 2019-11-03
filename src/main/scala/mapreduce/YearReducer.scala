package mapreduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class YearReducer extends Reducer[Text, IntWritable, Text, Text] {
  //reducer to calculate number of publications in a year

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {

    val sum = values.asScala.foldLeft(0)(_ + _.get)
    logger.info("number of publications in "+key+" is: "+sum)
    context.write(new Text(key.toString), new Text(sum.toString))
  }
}
