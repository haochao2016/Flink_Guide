package com.example.orderpay_detect

import com.example.domain.{OrderEvent, OrderResult}
import com.example.func.OrderPayMatchResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object OrderTimeOutWithoutCEP {
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

        val orderResultStream = orderEventStream
                .keyBy(_.orderId)
                .process(new OrderPayMatchResult())

        orderResultStream.print("payed")
        val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")
        orderResultStream.getSideOutput(orderTimeoutOutputTag).print("time-out")
        env.execute("order timeout without cep")
    }

}
