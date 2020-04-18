import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupid = "GRP1"
    val topics = "kafkaexample"
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("kafkastreaming")
    val ssc = new StreamingContext(sparkconf,Seconds(3))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val message = KafkaUtils.createDirectStream[String,String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams)
    )
    val line = message.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordsCount = words.map(x => (x, 1L)).reduceByKey(_+_)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
//    start zookeeper server
//    start kafka server
//    ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafkaexample
//    run this object
//    type "hello world" in kafka server

  }
}
