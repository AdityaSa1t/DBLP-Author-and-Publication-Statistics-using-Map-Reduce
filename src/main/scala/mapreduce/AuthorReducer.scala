package mapreduce

import java.lang

import org.apache.hadoop.io.{FloatWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


class AuthorReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] {
//Reducer class to accumulate the scores of a particular author(key)

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[FloatWritable], context: Reducer[Text, FloatWritable, Text, FloatWritable]#Context): Unit = {

    val sum = values.asScala.foldLeft(0f)(_ + _.get)
    logger.info("Computing authorship score for: ", key)
    context.write(key, new FloatWritable(sum))
  }
}







