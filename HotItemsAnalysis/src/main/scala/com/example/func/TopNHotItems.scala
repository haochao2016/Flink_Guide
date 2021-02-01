package com.example.func

import java.sql.Timestamp

import com.example.domain.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    private var itemViewListState: ListState[ItemViewCount] = _

    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
        itemViewListState.add(value)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def open(parameters: Configuration): Unit = {
        itemViewListState = getRuntimeContext.getListState(
            new ListStateDescriptor[ItemViewCount]("itemViewCount_list", classOf[ItemViewCount])
//            new ListStateDescriptor[ItemViewCount]("itemViewCount_list", new TypeHint[ItemViewCount](){}.getTypeInfo())
        )
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        val allItemViewCounts : ListBuffer[ItemViewCount] = ListBuffer()
        val iter = itemViewListState.get().iterator()
        while (iter.hasNext) {
            allItemViewCounts += iter.next()
        }
        val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topN)
        itemViewListState.clear()
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for( i <- sortedItemViewCounts.indices ){
            val currentItemViewCount = sortedItemViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                    .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
                    .append("热门度 = ").append(currentItemViewCount.count).append("\n")
        }
        result.append("\n==================================\n\n")

        Thread.sleep(1000)
        out.collect(result.toString())
    }
}
