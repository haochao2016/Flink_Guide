package com.example_flink.apitest

import com.example_flink.domain.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        val inputStream = env.socketTextStream("192.168.121.71", 7777)
        val resource = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(resource.getPath)
        val dataStream = inputStream
                .map(data => {
                    val arr = data.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
                })

        val highTempStream = dataStream.process(new SplitTempProcessor(30.0))

        highTempStream.print("high")
        highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")
        env.execute("side output test")
    }

}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {

    override def processElement(value: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {

        if (value.temperature > threshold) {
            out.collect(value)
        } else {
            ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
        }

    }
}
