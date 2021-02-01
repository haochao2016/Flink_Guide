package com.example.orderpay_detect

import com.example.domain.{OrderEvent, ReceiptEvent}
import com.example.func.{TxP, TxPayMatchResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchWithJoin {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource1 = getClass.getResource("/OrderLog.csv")
        val orderEventStream = env.readTextFile(resource1.getPath)
                .map(data => {
                    val arr = data.split(",")
                    OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                })
                .assignAscendingTimestamps(_.timestamp)
                .keyBy(_.txId)

        val resource2 = getClass.getResource("/ReceiptLog.csv")
        val receiptEventStream = env.readTextFile(resource2.getPath)
                .map(data => {
                    val arr = data.split(",")
                    ReceiptEvent(arr(0), arr(1), arr(2)toLong)
                })
                .assignAscendingTimestamps(_.timestamp)
                .keyBy(_.txId)
        val resultStream = orderEventStream
                        .intervalJoin(receiptEventStream)
                        .between(Time.seconds(-3), Time.seconds(5))
                        .process(new ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
                            override def processElement(left: OrderEvent,
                                                        right: ReceiptEvent,
                                                        ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                                        out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
                                out.collect((left, right))
                            }
                        })

        resultStream.print("matched")

//        resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-order")).print("unmatched order")
//        resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipt")

        env.execute("tx match job with join")
    }
}
