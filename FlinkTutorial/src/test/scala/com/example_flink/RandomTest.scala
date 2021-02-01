package com.example_flink

import java.util.Random

object RandomTest {

    def main(args: Array[String]): Unit = {
        val random = new Random()
        val curTemp = 1.to(10).map(i => random.nextDouble() * 100)
        println(curTemp)
        curTemp.foreach(i => println( 1 + random.nextGaussian()))
    }

}
