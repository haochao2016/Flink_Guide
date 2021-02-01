package com.example.func

import java.sql.Timestamp

import com.example.domain.PageViewCount
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNHotPages(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{

//    override def open(parameters: Configuration): Unit = super.open(parameters)
    lazy private val pageViewCountMapState : MapState[String, Long] =
        getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount,
                                ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                                out: Collector[String]): Unit = {
        pageViewCountMapState.put(value.url, value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60001L)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        if (timestamp == ctx.getCurrentKey + 60001) {
            pageViewCountMapState.clear()
            return
        }

        val listBuffer = new ListBuffer[(String, Long)]
        val iter = pageViewCountMapState.entries().iterator()
        while (iter.hasNext) {
            val value = iter.next()
            listBuffer += ((value.getKey, value.getValue))
        }

//        listBuffer.sortBy(_._2)(Ordering.Long.reverse).take(topN)
        val sortedPageViewCounts = listBuffer.sortWith(_._2 > _._2).take(topN)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")
        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for( i <- sortedPageViewCounts.indices ){
            val currentItemViewCount = sortedPageViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                    .append("页面URL = ").append(currentItemViewCount._1).append("\t")
                    .append("热门度 = ").append(currentItemViewCount._2).append("\n")
        }
        result.append("\n==================================\n")
        Thread.sleep(100)
        out.collect(result.toString())
    }

}
