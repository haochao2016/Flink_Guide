package com.example.func

import com.example.domain.ItemViewCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val windowEnd = window.getEnd
        val count = input.iterator.next()
        out.collect(ItemViewCount(key, windowEnd, count))
    }

}
