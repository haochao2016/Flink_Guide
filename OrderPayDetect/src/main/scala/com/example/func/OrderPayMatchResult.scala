package com.example.func

import com.example.domain.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class OrderPayMatchResult extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    // 定义状态，标识位表示create、pay是否已经来过，定时器时间戳
    lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
    // define the side output for timeout event
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {

        val isCreated = isCreatedState.value()
        val isPayed = isPayedState.value()
        val timer = timerTsState.value()

        if (value.eventType == "create") {
            // 1.1 如果已经支付过，正常支付，输出匹配成功的结果
            if (isPayed) {
                out.collect(OrderResult(value.orderId, "payed successfully"))
                isCreatedState.clear()
                isPayedState.clear()
                timerTsState.clear()
            } else {
                val ts = value.timestamp * 1000L + 5000L
                ctx.timerService().registerEventTimeTimer(ts)
                isCreatedState.update(true)
                timerTsState.update(ts)
            }
        } else if (value.eventType == "pay") {
            if (isCreated) {
                //would collect the record if time smaller than timer-timestamp
                if (value.timestamp * 1000L < timer) {
                    out.collect(OrderResult(value.orderId, "payed successfully"))
                } else {
                    ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
                }
                isCreatedState.clear()
                isPayedState.clear()
                timerTsState.clear()
            }
            // if not find the created event
            else {
                val ts = value.timestamp * 1000L + 3000L
                ctx.timerService().registerEventTimeTimer(ts)
                isPayedState.update(true)
                timerTsState.update(ts)
            }
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit = {

        if (isPayedState.value()) {
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
        } else if (isCreatedState.value()) {
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
    }
}