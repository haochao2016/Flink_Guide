package com.example.market_analysis

import com.example.domain.{AdClickLog, BlackListUserWarning}
import com.example.func.{AdCountAgg, AdCountWindowResult, FilterBlackListUserResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ADClickAnalysis {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val resource = getClass.getResource("/AdClickLog.csv")
        val inputStream = env.readTextFile(resource.getPath)
        val adLogStream = inputStream.map(data => {
            val arr = data.split(",")
            AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
        })
        .assignAscendingTimestamps(_.timestamp * 1000L)
        // 插入一步过滤操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
        val filterBlackListUserStream = adLogStream
//                .setParallelism(1)
                .keyBy(data => (data.userId, data.adId))
                .process(new FilterBlackListUserResult(100))

        val adCountResultStream = filterBlackListUserStream
                .keyBy(_.province)
                .timeWindow(Time.days(1), Time.hours(1))
                .aggregate(new AdCountAgg(), new AdCountWindowResult)

        adCountResultStream.print("ad")
        filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("waring")
        env.execute("AD click analysis")
    }
}
