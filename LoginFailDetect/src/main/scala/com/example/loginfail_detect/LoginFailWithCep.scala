package com.example.loginfail_detect

import java.util

import com.example.domain.{LoginEvent, LoginFailWarning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

import scala.collection.Map

object LoginFailWithCep {
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

        // 1. 定义匹配的模式， 要求是一个登录失败事件后，紧跟另一个登录失败事件
        val pattern = Pattern
                .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
                .next("secondFail").where(_.eventType == "fail")
                .next("thirdFail").where(_.eventType == "fail")
                .within(Time.seconds(5))

        val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)

                /*.process(new PatternProcessFunction[LoginEvent, LoginFailWarning] () {
                    override def processMatch(pattern /*`match`*/: util.Map[String, util.List[LoginEvent]],
                                              ctx: PatternProcessFunction.Context,
                                              out: Collector[LoginFailWarning]): Unit = {
                        val firstFail = pattern.get("firstFail").get(0)
                        val secondFail = pattern.get("secondFail").iterator().next()
                        val thirdFail = pattern.get("thirdFail").get(0)
                        out.collect(LoginFailWarning(firstFail.userId, firstFail.timestamp, thirdFail.timestamp,
                            "login fail 2 times in 2s"))
                    }
                })*/
                /*.select(new PatternSelectFunction[LoginEvent, LoginFailWarning] (){
                    override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
                        val firstFail = pattern.get("firstFail").get(0)
                        val secondFail = pattern.get("secondFail").iterator().next()
                        val thirdFail = pattern.get("thirdFail").get(0)
                        LoginFailWarning(firstFail.userId, firstFail.timestamp, thirdFail.timestamp,  "login fail 2 times in 2s")
                    }
                })*/
                .select((pattern: Map[String, Iterable[LoginEvent]]) => {
                    val firstFail = pattern.get("firstFail").get.iterator.next()
                    val secondFail = pattern.get("secondFail").get.iterator.next()
                    val thirdFail = pattern.get("thirdFail").get.iterator.next()
                    LoginFailWarning(firstFail.userId, firstFail.timestamp, thirdFail.timestamp,  "login fail 2 times in 2s")
                })

        patternStream.print
        env.execute("login fail with cep job")

    }
}
