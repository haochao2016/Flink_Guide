package com.example.func

import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UvTrigger() extends Trigger[(String, Long), TimeWindow]{

    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult =
        TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult =
        TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult =
        TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

}
