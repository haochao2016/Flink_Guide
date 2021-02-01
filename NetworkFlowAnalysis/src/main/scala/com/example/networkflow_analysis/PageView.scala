package com.example.networkflow_analysis

import com.example.domain.UserBehavior
import com.example.func.{PvCountAgg, PvCountWindowResult, TotalPvCountResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object PageView {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resourcePath = getClass.getResource("/UserBehavior.csv").getPath
        val inputStream = env.readTextFile(resourcePath)
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        })
        .assignAscendingTimestamps(_.timestamp * 1000L)

        val aggStream = dataStream
                .filter(_.behavior == "pv")
                .map(data => (Random.nextInt(10) + "pv", 1))
//                .map(data => ("pv", 1))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindowResult())

        /**
         * k1 :PvCount(1511661600000,41890)
         * k2 :PvCount(1511661600000,41890)
         * k3 :PvCount(1511661600000,41890)
         *
         */



        val resultStream = aggStream
                .keyBy(_.windowEnd)
                .sum("count")
//                .process(new TotalPvCountResult())

//        aggStream.print("agg")
        resultStream.print("agg")

        env.execute("page view job")
    }
}
