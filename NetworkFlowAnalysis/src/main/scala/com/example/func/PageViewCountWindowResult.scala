package com.example.func

import com.example.domain.PageViewCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
    override def apply(url: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(url, window.getEnd, input.iterator.next()))
    }
}
