package com.example.func

import org.apache.flink.api.common.functions.AggregateFunction

class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}
