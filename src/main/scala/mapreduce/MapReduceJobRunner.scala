package mapreduce

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.slf4j.{Logger, LoggerFactory}
import parser.XmlInputFormatWithMultipleTags

object MapReduceJobRunner {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    val conf = ConfigFactory.load("TagInfo.conf")

    //get start and end tags for each publication type
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))

    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

    configuration.set("mapred.textoutputformat.separator", ",")

    if (args(0).contentEquals("1")){
      //Job to get authorship score
      val authorScoreJob = Job.getInstance(configuration, "mapreduce_author")
      authorScoreJob.setJarByClass(this.getClass)
      //Setting mapper
      authorScoreJob.setMapperClass(classOf[AuthorMapper])
      authorScoreJob.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //setting reducer
      authorScoreJob.setReducerClass(classOf[AuthorReducer])
      authorScoreJob.setMapOutputKeyClass(classOf[Text])
      authorScoreJob.setMapOutputValueClass(classOf[FloatWritable])
      authorScoreJob.setOutputKeyClass(classOf[FloatWritable])
      authorScoreJob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(authorScoreJob, new Path(args(1)))
      FileOutputFormat.setOutputPath(authorScoreJob, new Path(args(2)))
      logger.debug("Setting up the Authorship score job..")
      authorScoreJob.waitForCompletion(true)
    }

    else if (args(0).contentEquals("2")){
      //Job to get the number of publications in a year
      val yearScoreJob = Job.getInstance(configuration, "mapreduce_year")
      yearScoreJob.setJarByClass(this.getClass)
      //Setting mapper
      yearScoreJob.setMapperClass(classOf[YearMapper])
      yearScoreJob.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //setting reducer
      yearScoreJob.setReducerClass(classOf[YearReducer])
      yearScoreJob.setMapOutputKeyClass(classOf[Text])
      yearScoreJob.setMapOutputValueClass(classOf[IntWritable])
      yearScoreJob.setOutputKeyClass(classOf[Text])
      yearScoreJob.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(yearScoreJob, new Path(args(1)))
      FileOutputFormat.setOutputPath(yearScoreJob, new Path(args(2))) //running this job
      logger.debug("Setting up the Year count job..")
      yearScoreJob.waitForCompletion(true)
    }

    else if(args(0).contentEquals("3")){
      //Job to get various statistics WRT to the number of co-authors
      val statsJob = Job.getInstance(configuration, "mapreduce_stats")
      statsJob.setJarByClass(this.getClass)
      //Setting mapper
      statsJob.setMapperClass(classOf[StatsMapper])
      statsJob.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //setting reducer
      statsJob.setReducerClass(classOf[StatsReducer])
      statsJob.setMapOutputKeyClass(classOf[Text])
      statsJob.setMapOutputValueClass(classOf[IntWritable])
      statsJob.setOutputKeyClass(classOf[Text])
      statsJob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(statsJob, new Path(args(1)))
      FileOutputFormat.setOutputPath(statsJob, new Path(args(2)))
      logger.debug("Setting up the Stats job..")
      statsJob.waitForCompletion(true)
    }

    else if(args(0).contentEquals("4")){
      //Job to get the number of publication venues published each year
      val publicationJob = Job.getInstance(configuration, "mapreduce_publications")
      publicationJob.setJarByClass(this.getClass)
      //Setting mapper
      publicationJob.setMapperClass(classOf[PublicationMapper])
      publicationJob.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //setting reducer
      publicationJob.setReducerClass(classOf[PublicationReducer])
      publicationJob.setMapOutputKeyClass(classOf[Text])
      publicationJob.setMapOutputValueClass(classOf[IntWritable])
      publicationJob.setOutputKeyClass(classOf[Text])
      publicationJob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(publicationJob, new Path(args(1)))
      FileOutputFormat.setOutputPath(publicationJob, new Path(args(2)))
      logger.debug("Setting up the stratified year/publication type job..")
      publicationJob.waitForCompletion(true)
    }

  }
}