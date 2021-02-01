package com.example.domain

// define input case class
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// define window aggregation output case class
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)