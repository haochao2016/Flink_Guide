import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer_HotItems {

    def main(args: Array[String]): Unit = {
        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "192.168.121.71:9092")
        prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](prop)
        val source = scala.io.Source.fromFile("D:\\workSpace\\Idea\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        source.getLines()
        for (line <- source.getLines()) {
            val record = new ProducerRecord[String, String]("HotItems", line)
            producer.send(record)
            Thread.sleep(2)
        }
        producer.close()
        println("=================Send Over=================")
    }

}
