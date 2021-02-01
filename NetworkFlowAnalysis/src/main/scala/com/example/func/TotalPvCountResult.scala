package com.example.func

import com.example.domain.PvCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {

    lazy private val totalValueState : ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("total-value", classOf[Long])
    )

    override def processElement(value: PvCount,
                                ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context,
                                out: Collector[PvCount]): Unit = {
        val count = totalValueState.value()
        totalValueState.update(count + value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext,
                         out: Collector[PvCount]): Unit = {
        val totalValue = totalValueState.value()
        out.collect(PvCount(ctx.getCurrentKey, totalValue))
        totalValueState.clear()
    }

}
