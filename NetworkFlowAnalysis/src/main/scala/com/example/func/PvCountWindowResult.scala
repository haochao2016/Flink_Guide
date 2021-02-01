package com.example.func

import com.example.domain.PvCount
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.function.WindowFunction

class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
//        out.collect(PvCount(window.getEnd, input.head))
        out.collect(PvCount(window.getEnd, input.iterator.next()))
    }
}
