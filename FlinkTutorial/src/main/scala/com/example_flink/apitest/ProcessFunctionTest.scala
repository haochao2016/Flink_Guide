package com.example_flink.apitest

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(resource.getPath)

        val dataStream = inputStream.map( data => {
            val arr = data.split(",")
            //                (id: String, timestamp: Long, temperature: Double)
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
                .keyBy(_.id)
                .process(new TempIncreWarning(10000L))
                        .print()

        env.execute("ProcessFunctionTest")
    }

}
class TempIncreWarning(interval: Long) extends  KeyedProcessFunction[String, SensorReading, String] {

    // save last time temperature value
    lazy val lastTempValueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
        val lastTemp = lastTempValueState.value()
        val time = timerTsState.value()
//        println(s"time : $time")
//        println(s"lastTemp : $lastTemp")
        lastTempValueState.update(value.temperature)

        if (value.temperature <= lastTemp) {
            ctx.timerService().deleteEventTimeTimer(time)
            timerTsState.clear()
        }  else if (value.temperature > lastTemp && time == 0) {
            val ts = ctx.timerService().currentProcessingTime() + interval
            ctx.timerService().registerEventTimeTimer(ts)
            timerTsState.update(ts)
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval/1000L + "秒连续上升。")
        timerTsState.clear()
    }
}
