package com.example_flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
//        env.disableOperatorChaining()

        val parameterTool = ParameterTool.fromArgs(args)
        val host = parameterTool.get("host")   // 192.168.121.71
        val port = parameterTool.get("port").toInt  // 7777

        val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
        inputDataStream
                .flatMap(_.split(" "))
                .filter(_.nonEmpty)
                .map((_, 1))
                .keyBy(_._1)
                .sum(1)
                .print()

        env.execute("stream word count")
    }
}
