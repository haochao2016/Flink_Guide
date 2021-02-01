package com.example.orderpay_detect

import com.example.domain.{OrderEvent, ReceiptEvent}
import com.example.func.TxPayMatchResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object TxMatch {
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
                .connect(receiptEventStream)
                .process(new TxPayMatchResult())

        resultStream.print("matched")

        resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-order")).print("unmatched order")
        resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipt")

        env.execute("tx match job")
    }
}
