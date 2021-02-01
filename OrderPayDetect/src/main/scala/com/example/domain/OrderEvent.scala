package com.example.domain

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

case class OrderResult(ordering: Long, resultMsg: String)