package com.example.func

import java.util.{Random, UUID}

import com.example.domain.MarketUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class SimulatedSource() extends RichSourceFunction[MarketUserBehavior]{
    // if running flag
    val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
    val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
    var running = true

    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        val maxCount = Long.MaxValue
        var count = 0L

        while (running && count < maxCount) {
            //userId: String, behavior: String, channel: String, timestamp: Long
            val userId = UUID.randomUUID().toString
            val rand = new Random()
            ctx.collect(MarketUserBehavior(userId,
                behaviorSet(rand.nextInt(behaviorSet.size)),
                channelSet(rand.nextInt(channelSet.size)),
                System.currentTimeMillis()))
            count += 1
            Thread.sleep(50)
        }
    }

    override def cancel(): Unit = running = false
}
