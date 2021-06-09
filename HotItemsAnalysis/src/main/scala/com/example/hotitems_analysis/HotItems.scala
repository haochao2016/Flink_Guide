package com.example.hotitems_analysis

import java.util.Properties

import com.example.domain.{ItemViewCount, UserBehavior}
import com.example.func.{CountAgg, ItemViewWindowResult, TopNHotItems}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object HotItems {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        env.setStateBackend()
//        env.setStateBackend(new RocksDBStateBackend(""))

      /*  val UserBehaviorPath = "D:\\workSpace\\Idea\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(UserBehaviorPath)
        */

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "192.168.121.71:9092")
        properties.setProperty("group .id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        val inputStream = env.addSource(new FlinkKafkaConsumer[String]( "HotItems",
            new SimpleStringSchema(), properties))
        val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
            val arr = data.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000L)

        val aggStream: DataStream[ItemViewCount] = dataStream
            .filter(_.behavior == "pv")
            .keyBy(_.itemId)
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new ItemViewWindowResult())

        val resultStream = aggStream
                .keyBy(_.windowEnd)
                .process(new TopNHotItems(5))

//        dataStream.print("dataSource")
        aggStream.print("agg")
        resultStream.print("result")

        env.execute("HotItems")
    }
}
