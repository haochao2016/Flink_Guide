package com.example_flink.apitest

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        env.setStateBackend(new FsStateBackend("", true))
        env.setStateBackend(new RocksDBStateBackend("file:///D:/workSpace/Idea/FlinkTutorial/src/main/resources/stateBackend"))

        // open checkpoint
        env.enableCheckpointing()
        // checkpoint的配置
        val chkpConfig = env.getCheckpointConfig
        chkpConfig.setCheckpointInterval(10000L)
        chkpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        chkpConfig.setCheckpointTimeout(60000)
        chkpConfig.setMaxConcurrentCheckpoints(2)
        chkpConfig.setMinPauseBetweenCheckpoints(500L)
        chkpConfig.setPreferCheckpointForRecovery(true)
        chkpConfig.setTolerableCheckpointFailureNumber(0)

        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(10)))
        val resource = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(resource.getPath)
        // 先转换成样例类类型（简单转换操作）
        val dataStream = inputStream
                .map( data => {
                    val arr = data.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
                } )
                .uid("1")

        val alertStream = dataStream
                .keyBy(_.id)
//                .flatMap[(String, Double, Double)](new TempChangeAlert(10.0))
                .flatMapWithState[(String, Double, Double), Double]{
                    case (data: SensorReading, None) =>  ( List.empty, Some(data.temperature) )
                    case (data: SensorReading, lastTemp: Option[Double]) => {
                        val diff = (data.temperature - lastTemp.get).abs
                        if (diff > 10.0) ( List((data.id, lastTemp.get, data.temperature)), Some(data.temperature) )
                        else (List.empty, Option(data.temperature))
                    }
                }

        alertStream.print()

        env.execute("state test")
    }
}

class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val flagState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("Flag", classOf[Boolean]))

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
        val lastTemp = lastTempState.value()
        if (!flagState.value()) println(flagState.value())
        val diff = (value.temperature - lastTemp).abs
        if (diff > threshold && !flagState.value()) {
            out.collect((value.id, lastTemp, value.temperature))
            lastTempState.update(value.temperature)
            flagState.update(true)
        }

    }
}
