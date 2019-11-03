package mapreduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class StatsReducer extends Reducer[Text, IntWritable, Text, Text] {
  //Reducer class to calculate the maximum, median and average number of co-authors a particular author

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {

    var lst = ArrayBuffer[Float]()

    for (y <- values.asScala){
      lst += y.get().toFloat
    }

    lst = lst.sorted // Sorting the list to get stats

    var median: Float = 0f
    var avg: Float = 0f
    if(lst.length % 2 == 0 && lst.length !=0 ){ //when total is even
      val index = (lst.length)/2
      median = (lst(index)+lst(index-1))/2
      avg = lst.sum/lst.length
      context.write(new Text(key), new Text(lst.max.toInt + "," + median.toInt + "," + avg))
    } else if (lst.length == 1){ //when total is 1
      median = lst(0)
      avg = lst(0)
      context.write(new Text(key), new Text(lst.max.toInt + "," + median.toInt + "," + avg))
    } else if( lst.length == 0) { //when th author has no co-authors
      median = 0
      avg = 0
      context.write(new Text(key), new Text(0 + "," + 0 + "," + avg))
    } else { //all other general cases
      median = lst(lst.length/2)
      avg = lst.sum/lst.length
      context.write(new Text(key), new Text(lst.max.toInt + "," + median.toInt + "," + avg))
    }

    logger.debug("MAX number of co-authors in a paper="+lst.max+"\t MEDIAN number of co-authors in a paper ="
      +median+"\t AVG number of co-authors in a paper="+ avg)

  }
}
