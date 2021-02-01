package com.example.hotitems_analysis

import com.example.domain.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object NewItemsWithSql {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val path = "D:\\workSpace\\Idea\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(path)
        val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build()

        val tEnv = StreamTableEnvironment.create(env, settings)
        val dataTable = tEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
        val aggTable = dataTable
                .filter($"behavior" === "pv")
                .window(Slide over 1.hours() every 5.minutes() on 'ts as 'sw)
                .groupBy($"sw", $"itemId")
                .select($"itemId", $"sw".end().as("windowEnd"), $"itemId".count().as("cnt"))

        tEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)

        val resultTable = tEnv.sqlQuery(
            """
              | select *
              | from (
              |     select *, row_number() over
              |         (partition by windowEnd order by cnt desc) as row_num
              |     from aggTable)
              |  where row_num <= 5
              |""".stripMargin)
        resultTable.toRetractStream[Row].print()

        // whole sql expression
       /* tEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
        tEnv.sqlQuery(
            """
              | select *
              | from (
              |     select *, row_number() over
              |         (partition by windowEnd order by cnt desc) as row_num
              |     from (
              |         select
              |          itemId,
              |          hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
              |          count(itemId) as cnt
              |        from
              |         dataTable
              |         where behavior = 'pv'
              |         group by
              |           itemId,
              |           hop(ts, interval '5' minute, interval '1' hour)
              |     ))
              |  where row_num <= 5
              |""".stripMargin)
        .toRetractStream[Row].print("hot items whole sql job")
        */

        env.execute("hot items sql job")
    }
}
