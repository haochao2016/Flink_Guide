package com.example_flink.apitest.sinktest

import java.util

import com.example_flink.domain.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.mutable

object EsSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val resource = getClass.getResource("/sensor.txt")
        val inputStream = env.readTextFile(resource.getPath)

        val dataStream = inputStream
                .map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
            override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                // 包装为 一个 Map 作为 data source
                val dataSource = new util.HashMap[String, String]()
                dataSource.put("id", element.id)
                dataSource.put("temperature", element.temperature.toString)
                dataSource.put("timestamp", element.timestamp.toString)

                val request = Requests.indexRequest("sensor")
                        //                        .index()
                        .source(dataSource)
                indexer.add(request)
            }
        }

        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("192.168.121.71", 9200))
        dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFunc).build())
        /*   //构造方法私有化
        val blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build()
                */
//        http://192.168.121.71:9200/sensor/_search?pretty
        env.execute("ES sink test")
    }

}
