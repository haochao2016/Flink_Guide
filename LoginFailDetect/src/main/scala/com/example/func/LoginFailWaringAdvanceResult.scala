package com.example.func

import com.example.domain.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class LoginFailWaringAdvanceResult extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{

    lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(
        new ListStateDescriptor[LoginEvent]("login-list", classOf[LoginEvent]))

    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                                out: Collector[LoginFailWarning]): Unit = {

        if (value.eventType == "fail") {
//            loginFailState.add(value)
            val iterLoginFail = loginFailState.get().iterator()
            if (iterLoginFail.hasNext) {
                val lastFailTime = iterLoginFail.next().timestamp
                if (lastFailTime + 2 > value.timestamp) {
                    out.collect(LoginFailWarning(value.userId, lastFailTime, value.timestamp, "login fail 2 times in 2s"))
                }
                loginFailState.clear()
            }
            loginFailState.add(value)
        } else {
            loginFailState.clear()
        }
    }
}
