package com.example.func

import com.example.domain.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{

    lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    val unmatchedOrderEventOutputTag = new OutputTag[OrderEvent]("unmatched-order")
    val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")

    override def processElement1(order: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        val receipt = receiptState.value()
        if (receipt != null) {
            out.collect((order, receipt))
            receiptState.clear()
            orderState.clear()
        } else {
            ctx.timerService().registerEventTimeTimer(order.timestamp * 1000L + 5000L)
            orderState.update(order)
        }
    }

    override def processElement2(receipt: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        val order = orderState.value()
        if (order != null) {
            out.collect((order, receipt))
            receiptState.clear()
            orderState.clear()
        } else {
            ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
            receiptState.update(receipt)
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        if (receiptState.value() != null) {
            ctx.output(unmatchedReceiptEventOutputTag, receiptState.value())
        }
        if (orderState.value() != null) {
            ctx.output(unmatchedOrderEventOutputTag, orderState.value())
        }
        receiptState.clear()
        orderState.clear()
    }
}

class TxP extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement1(value: OrderEvent,
                                 ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit ={
    }

    override def processElement2(value: ReceiptEvent,
                                 ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    }
}