package com.example_flink.apitest

import java.util.{Properties, Random}

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}

object SourceTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        readFromCollection(env)
//        readFromFile(env)
//        readDataFromKafka(env)
        selfDefineSource(env)

        env.execute("source test")
    }

    def selfDefineSource (env: StreamExecutionEnvironment): Unit = {
        val stream = env.addSource(new MySourceFunction)
        stream.print()
    }

    def readDataFromKafka (env: StreamExecutionEnvironment): Unit = {
        val properties = new Properties()
        properties.put("bootstrap.servers", "192.168.121.71:9092")
        properties.put("group.id", "consumer-group")
        val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
        stream3.print()
    }

    def readFromFile (env: StreamExecutionEnvironment) = {
        val url = getClass.getResource("/sensor.txt")
        val stream2 = env.readTextFile(url.getPath)
        stream2.print()
    }

    def readFromCollection (env: StreamExecutionEnvironment): Unit = {
        val dataList = List(SensorReading("sensor_1", 1547718199, 35.8)
            , SensorReading("sensor_6", 1547718201, 15.4)
            , SensorReading("sensor_7", 1547718202, 6.7)
            , SensorReading("sensor_10", 1547718205, 38.1)
        )
        val stream = env.fromCollection(dataList)
        stream.print()
    }

}

class MySourceFunction extends SourceFunction[SensorReading] {

    var running = true

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
//      SensorReading(id: String, timestamp: Long, temperature: Double)
        val random = new Random()
        var curTemp = 1.to(10).map(i => ("sensor" + i, random.nextDouble() * 100))

        while (running) {
            curTemp = curTemp.map(data => (data._1, data._2 + random.nextGaussian()))
            val curTime = System.currentTimeMillis()
            curTemp.foreach(data => {
                ctx.collect(SensorReading(data._1, curTime, data._2))
            })
            Thread.sleep(100)
        }
    }

    override def cancel(): Unit = running = false
}
