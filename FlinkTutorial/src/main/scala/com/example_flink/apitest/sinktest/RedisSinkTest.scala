package com.example_flink.apitest.sinktest

import com.example_flink.domain.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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

        val config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build()

        dataStream.addSink(new RedisSink[SensorReading](config, new MyRedisMapper))

        env.execute("redis sink test")
    }

}

class MyRedisMapper extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
    }

    override def getKeyFromData(data: SensorReading): String = data.temperature.toString

    override def getValueFromData(data: SensorReading): String = data.id
}
