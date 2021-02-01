package com.example.func

import java.sql.Timestamp

import com.example.domain.{MarketUserBehavior, MarketViewCount}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MarketCountByChannel
        extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow]{

    override def process(key: (String, String),
                         ctx: Context,
                         elements: Iterable[MarketUserBehavior],
                         out: Collector[MarketViewCount]): Unit ={

        //windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long
        val windowStart = new Timestamp(ctx.window.getStart).toString
        val windowEnd = new Timestamp(ctx.window.getEnd).toString
        out.collect(MarketViewCount(windowStart,
                                windowEnd,
                                key._2,
                                key._1,
                                elements.size))
    }

}
