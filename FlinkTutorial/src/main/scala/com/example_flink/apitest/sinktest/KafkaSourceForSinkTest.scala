package com.example_flink.apitest.sinktest

import java.util.Properties

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object KafkaSourceForSinkTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        testWriteToKafka(env)
        env.execute("kafka sink test")
    }

    def testWriteToKafka(env: StreamExecutionEnvironment)  = {
        val resource = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(resource.getPath)
        val dataStream = inputStream
                .map(data => {
                    val arr = data.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
                })

        val properties = new Properties()
        properties.put("bootstrap.servers",  "192.168.121.71:9092")
        dataStream.addSink(new FlinkKafkaProducer[String]("sensor", new SimpleStringSchema(), properties))
        println("*********** Write Over ************")
    }
}
