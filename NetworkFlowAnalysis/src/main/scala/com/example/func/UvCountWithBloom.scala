package com.example.func

import com.example.domain.UvCount
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction.Context

/**
 * User defined the window process function with Bloom Filter
 */
class UvCountWithBloom
        extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]
//        with KeyedProcessFunction[String, (String, Long), UvCount]
{

    // 定义redis连接器以及布隆过滤器
    lazy val jedis = new Jedis("localhost", 6379)
    private val bloom = new Bloom(1 << 30)

/*    Long.MaxValue
      9,223,372,036,854,775,807  */


    override def process(key: String,
                         context: /*ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]#*/Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {

        //先定义redis 中存储位图的的key
        val currentKey = context.window.getEnd.toString
        // 另外将当前窗口的uv count值，作为状态保存到redis里，用一个叫做uvcount的hash表来保存（windowEnd，count）
        val uvCountMap = "uvCount"
        var count = 0L
        if (jedis.hget(uvCountMap, currentKey) != null)
            count = jedis.hget(uvCountMap, currentKey).toLong

        val userId = elements.last._2.toString
        val offset = bloom.hash(userId, 61)
        // if exist , return true
        val isExist = jedis.getbit(currentKey, offset)
        if (!isExist) {
            jedis.setbit(currentKey, offset, true)
            jedis.hset(uvCountMap, currentKey, (count + 1).toString)
        }

    }

}
