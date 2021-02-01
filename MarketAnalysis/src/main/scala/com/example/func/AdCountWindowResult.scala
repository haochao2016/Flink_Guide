package com.example.func

import java.sql.Timestamp

import com.example.domain.AdClickCountByProvince
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdCountWindowResult extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow]{

    override def apply(key: String, window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[AdClickCountByProvince]): Unit = {
        //windowEnd: String, province: String, count: Long
        val ts = new Timestamp(window.getEnd)
        out.collect(AdClickCountByProvince(ts.toString, key, input.head))
    }
}
