package com.example_flink.apitest

import com.example_flink.domain.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter

object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.getConfig.setAutoWatermarkInterval(50)

        val url = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(url.getPath)
//        val inputStream = env.socketTextStream("192.168.121.71", 7777)
        val lateTag = new OutputTag[SensorReading]("late")

        val dataStream = inputStream
                .map(s => {
                    val arr = s.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy[SensorReading](
                    new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
                        override def extractTimestamp(element: SensorReading): Long = element.timestamp
                }))
                .keyBy(_.id)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .reduce((curRes, newData) => SensorReading(curRes.id, curRes.timestamp.min(newData.timestamp), newData.temperature))

        dataStream.print("res")
        dataStream.getSideOutput(lateTag).print("late")
        env.execute("window test")
    }

}
