package com.example_flink.apitest.tabletest

import com.example_flink.domain.SensorReading
import org.apache.flink.streaming.api.scala._

object Example {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据
        val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
        val inputStream = env.readTextFile(inputPath)
        //    val inputStream = env.socketTextStream("localhost", 7777)

        // 先转换成样例类类型（简单转换操作）
        val dataStream = inputStream
            .map(data => {
                val arr = data.split(",")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })

    }

}
