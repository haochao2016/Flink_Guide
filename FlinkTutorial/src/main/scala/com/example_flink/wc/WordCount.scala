package com.example_flink.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val resource = getClass.getResource("/hello.txt")
    val inputDataSet: DataSet[String] = env.readTextFile(resource.getPath)

    val resultDataSet: DataSet[(String, Int)] = inputDataSet
            .flatMap(_.split(" "))
            .map((_, 1))
            .groupBy(0)
            .sum(1)

    resultDataSet.print()
  }
}
