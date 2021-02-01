package com.example_flink.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.example_flink.domain.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {

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

        dataStream.addSink(new MyMysqlSinkFunction())

        env.execute()
    }
}

class MyMysqlSinkFunction extends RichSinkFunction[SensorReading] {

    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=UTC", "root", "123456")
        insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
        updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()
      // 如果更新没有查到数据，那么就插入
      if( updateStmt.getUpdateCount == 0 ){
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }

}
