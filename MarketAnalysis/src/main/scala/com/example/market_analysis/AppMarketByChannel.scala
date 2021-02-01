package com.example.market_analysis

import com.example.func.{MarketCountByChannel, SimulatedSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AppMarketByChannel {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val dataStream = env.addSource(new SimulatedSource())
                .assignAscendingTimestamps(_.timestamp)
                .filter(_.behavior != "uninstall")
                .keyBy(data => (data.behavior, data.channel))
                .timeWindow(Time.days(1), Time.seconds(10))
                // All Window Function, count in once
                .process(new MarketCountByChannel())

        dataStream.print("app")
        env.execute("app market channel counting")
    }
}
