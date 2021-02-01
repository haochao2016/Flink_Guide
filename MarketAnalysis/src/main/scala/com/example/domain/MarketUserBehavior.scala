package com.example.domain

//import org.apache.flink.table.planner.expressions.WindowStart
//import org.apache.flink.table.planner.expressions.WindowEnd

// define the input case class for user behavior message
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//define the output case class for user behavior message
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//define the input case class for AD click log
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//define the out case class for province click event count
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

//define the case class for Side Output Stream with black list
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)
