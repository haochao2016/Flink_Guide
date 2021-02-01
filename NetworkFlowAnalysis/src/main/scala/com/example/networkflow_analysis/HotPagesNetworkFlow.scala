package com.example.networkflow_analysis

import java.text.SimpleDateFormat

import com.example.domain.ApacheLogEvent
import com.example.func.{PageCountAgg, PageViewCountWindowResult, TopNHotPages}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter

object HotPagesNetworkFlow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val filePath = "D:\\workSpace\\Idea\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log"
//        val inputStream = env.readTextFile(filePath)
        val inputStream = env.socketTextStream("192.168.121.71", 7777)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        // convert the data type and assigner watermark
        val dataStream = inputStream.map(
            line => {
                val arr = line.split(" ")
                val ts: Long = sdf.parse(arr(3)).getTime
                ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
            })
            .assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarksAdapter.Strategy[ApacheLogEvent](
                    new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
                        override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
                    }
                ))

        // open a window
        val aggStream = dataStream
                .filter(_.method == "GET")
                .keyBy(_.url)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

        val resultStream = aggStream
                .keyBy(_.windowEnd)
                .process(new TopNHotPages(3))

        dataStream.print("data")
        aggStream.print("agg")
        aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
        resultStream.print()
        env.execute("hot pages job")
    }
}
