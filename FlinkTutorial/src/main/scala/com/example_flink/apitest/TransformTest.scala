package com.example_flink.apitest

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object TransformTest {

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

       /* dataStream
//                .filter(_.id.startsWith("sensor_1"))
                .keyBy(_.id)
                .min(2)
//                .minBy(2)
                .print()*/

        /*dataStream
                .keyBy(_.id)
//                .reduce((curData, newData) => {
//                    SensorReading(curData.id, newData.timestamp, curData.temperature + newData.temperature)
//                })
                .reduce(new MyReduceFunction)
                .print()*/

        val splitStream = dataStream
                .split(sensor => if (sensor.temperature > 30) Seq("high") else Seq("low"))

        val highTempStream = splitStream.select("high")
        val lowTempStream = splitStream.select("low")
        val allTempStream = splitStream.select("high", "low")

//        highTempStream.print("high")
//        lowTempStream.print("low")
//        allTempStream.print("all")

        val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = highTempStream
                .map(data => (data.id, data.temperature))
                .connect(lowTempStream)

        val coMapResultStream: DataStream[Any] = connectedStreams.map(
            warningData => (warningData._1, warningData._2, "warning"),
            lowTempStream => (lowTempStream.id, "healthy")
        )

        val unionStream = highTempStream.union(lowTempStream, allTempStream).print("union")

        coMapResultStream.print("coMap")

        env.execute("transform test")
    }

}

class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value2.timestamp, value1.temperature + value2.temperature)
    }
}