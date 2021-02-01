package com.example.func

import java.util

import com.example.domain.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternSelectFunction

class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult]{

    override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
        val orderEvent = pattern.get("create").iterator().next()
        val orderId = orderEvent.orderId
        val startTs = orderEvent.timestamp
        val endTs = pattern.get("pay").iterator().next().timestamp

        OrderResult(orderId, s"startTime: $startTs, endTime: $endTs, payed successfully")
    }
}
