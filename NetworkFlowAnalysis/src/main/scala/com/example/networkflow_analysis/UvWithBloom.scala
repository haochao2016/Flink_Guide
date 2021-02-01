package com.example.networkflow_analysis

import com.example.domain.UserBehavior
import com.example.func.{UvCountWithBloom, UvTrigger}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger

object UvWithBloom {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val resource = getClass.getResource("/UserBehavior.csv")
        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val uvStream = dataStream
                .filter(_.behavior == "pv")
                .map(data => ("uv", data.userId))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .trigger(new UvTrigger())
//                .trigger(new EventTimeTrigger())
                .process(new UvCountWithBloom())

        uvStream.print()
        env.execute("UV Count Bloom job")
    }
}
