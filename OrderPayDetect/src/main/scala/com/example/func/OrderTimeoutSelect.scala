package com.example.func

import java.util

import com.example.domain.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternTimeoutFunction

class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {

    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
        val orderId = pattern.get("create").get(0).orderId
        val startTs = pattern.get("create").get(0).timestamp
        OrderResult(orderId, s"startTime: $startTs,  timeout: $timeoutTimestamp")
    }
}
