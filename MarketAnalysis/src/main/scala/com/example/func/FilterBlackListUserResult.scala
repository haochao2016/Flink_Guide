package com.example.func

import com.example.domain.{AdClickLog, BlackListUserWarning}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class FilterBlackListUserResult(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]{

    lazy val countState: ValueState[Int] = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("count", classOf[Int]))

    lazy val resetTimerState : ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))

    lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(
        new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

    override def processElement(value: AdClickLog,
                                ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context,
                                out: Collector[AdClickLog]): Unit = {
        val currentValue = countState.value()
//        判断只要第一条数据来了， 就注册0点清除状态定时器
        if (currentValue == 0) {
            val purgeTs = (ctx.timerService().currentProcessingTime() / (1000 * 3600 * 24) + 1) * (24 * 3600 * 1000) - 8 * 3600 * 1000
            resetTimerState.update(purgeTs)
            ctx.timerService().registerProcessingTimeTimer(purgeTs)
        }

//         判断count 值是否达到定义的阈值， 如果超出阈值就输出到黑名单
        if(currentValue >= maxCount) {
            // 判断是否已经在黑名单里，没有的话才输出侧输出流
            if(!isBlackState.value()) {
                isBlackState.update(true)
                ctx.output(new OutputTag[BlackListUserWarning]("warning"),
                    BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))
            }
            return
        }

        // 正常情况，count加1，然后将数据原样输出
        countState.update(currentValue + 1)
//        println(value.userId + " ,  " + value.adId + ", "+ countState.value())
        out.collect(value)

    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                         out: Collector[AdClickLog]): Unit = {
        if(timestamp == resetTimerState.value()) {
            isBlackState.clear()
            countState.clear()
        }
    }
}
