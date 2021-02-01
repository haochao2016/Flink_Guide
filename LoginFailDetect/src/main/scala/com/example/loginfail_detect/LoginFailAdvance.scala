package com.example.loginfail_detect

import com.example.domain.LoginEvent
import com.example.func.{LoginFailWaringAdvanceResult, LoginFailWarningResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter

object LoginFailAdvance {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val resource = getClass.getResource("/LoginLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val loginEventStream = inputStream.map(data => {
            val arr = data.split(",")
            LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
        })
        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy[LoginEvent](
            new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
            }
        ))
        // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
        val loginFailWarningStream = loginEventStream
                .keyBy(_.userId)
                .process(new LoginFailWaringAdvanceResult)

        loginFailWarningStream.print
        env.execute("login fail detect job")
    }
}
