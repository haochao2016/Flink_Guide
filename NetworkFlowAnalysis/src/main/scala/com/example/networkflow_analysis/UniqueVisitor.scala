package com.example.networkflow_analysis

import com.example.domain.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable.HashSet

object UniqueVisitor {
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
                .timeWindowAll(Time.hours(1))
                .apply(new AllWindowFunction[UserBehavior, UvCount, TimeWindow]() {
                    override def apply(window: TimeWindow,
                                       input: Iterable[UserBehavior],
                                       out: Collector[UvCount]): Unit = {
                        var userIds = new HashSet[Long]()
                        for (ub <- input.iterator)
                            userIds += ub.userId
                        out.collect(UvCount(window.getEnd, userIds.size))
                    }
                })
        uvStream.print("UV")

        env.execute("UvCount job")
    }
}
