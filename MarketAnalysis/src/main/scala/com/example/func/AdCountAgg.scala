package com.example.func

import com.example.domain.AdClickLog
import org.apache.flink.api.common.functions.AggregateFunction

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}