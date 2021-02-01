package com.example.orderpay_detect

import com.example.domain.{OrderEvent, OrderResult}
import com.example.func.{OrderPaySelect, OrderTimeoutSelect}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object OrderTimeout {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/OrderLog.csv")
        val orderEventStream = env.readTextFile(resource.getPath)
//        val orderEventStream = env.socketTextStream("192.168.121.71", 7777)
            .map(data => {
                val arr = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignAscendingTimestamps(_.timestamp)
            .keyBy(_.orderId)

        val pattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
                .followedBy("pay").where(_.eventType == "pay")
                .within(Time.minutes(15))

        val outPutTag = new OutputTag[OrderResult]("orderTimeOut")

        val resultStream = CEP.pattern(orderEventStream, pattern)
//                    .select(outPutTag, new OrderTimeoutSelect, new OrderPaySelect)
                .select(outPutTag) ((orderMap: Map[String, Iterable[OrderEvent]], timeoutTimestamp : Long)  => {
                    val orderId = orderMap.get("create").get.head.orderId
                    val startTs = orderMap.get("create").get.head.timestamp
                    OrderResult(orderId, s"startTime: $startTs, timeOut: $timeoutTimestamp")
                }) ((map: Map[String, Iterable[OrderEvent]]) => {
                    val orderId = map.get("create").get.head.orderId
                    val startTs = map.get("create").get.head.timestamp
                    val endTs = map.get("pay").get.head.timestamp
                    OrderResult(orderId, s"startTime: $startTs, endTime: $endTs, payed successfully")
                })

        resultStream.print("payed")
        resultStream.getSideOutput[OrderResult](outPutTag).print("timeout")

        env.execute("order timeout job")
    }
}
