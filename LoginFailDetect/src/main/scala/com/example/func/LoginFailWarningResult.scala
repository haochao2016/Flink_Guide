package com.example.func

import com.example.domain.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{

    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
        new ListStateDescriptor[LoginEvent]("login-list", classOf[LoginEvent]))

    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                                out: Collector[LoginFailWarning]): Unit = {

        if (value.eventType == "fail") {
            loginState.add(value)
            if (timerState.value() == 0) {
                val ts = (value.timestamp + 2) * 1000L
                ctx.timerService().registerEventTimeTimer(ts)
                timerState.update(ts)
            }
        } else {
            ctx.timerService().deleteEventTimeTimer(timerState.value())
            loginState.clear()
            timerState.clear()
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext,
                         out: Collector[LoginFailWarning]): Unit = {
        if (timestamp != timerState.value())
            return
        val allLoginFailList = new ListBuffer[LoginEvent]()
        loginState.get().forEach(data => allLoginFailList += data)

        if (allLoginFailList.size >= failTimes) {
            out.collect(LoginFailWarning(
                allLoginFailList.head.userId,
                allLoginFailList.head.timestamp,
                allLoginFailList.last.timestamp,
                "login fail in 2s for " + allLoginFailList.length + " times."))
        }
        loginState.clear()
        timerState.clear()
    }
}
