import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DataEngineering_Streaming {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

    def main (args: Array[String]) {
      val ssc = new StreamingContext("local[3]", "PopularHashtags", Seconds(1))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "54.210.214.162:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array("tweets")

      val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

      val lines = stream.map(_.value)

      lines.print()

      val linesFilter = lines
        .map(_.toLowerCase)
        .filter(_.split("\\,").length >=1 )
        .map(x =>{
          val y = x.split("\\,")
          (y(1))})
      linesFilter.print()
      lines.print()


      // Split using a regular expression that extracts words
      val words = linesFilter.flatMap(x => x.split("\\W+"))
      //words.print()
      // Normalize everything to lowercase
      val lowercaseWords = words.map(x => x.toLowerCase())
      lowercaseWords.print()

      val lowercaseWordsFilter = lowercaseWords.filter(_.length > 0)
      lowercaseWordsFilter.print()
      // Count of the occurrences of each word
      //val wordCounts = lowercaseWords.countByValue()
      val wordPairs = lowercaseWordsFilter.map(x => (x, 1))
      wordPairs.print()

      val wordCounts = lowercaseWordsFilter.countByValue()
      wordCounts.print()

      val wordCounts1 = wordPairs.reduceByKey(_ + _)
      wordCounts1.print()

      //ssc.checkpoint("C:/checkpoint/")
      ssc.start()
      ssc.awaitTermination()




    }
}
