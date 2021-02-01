package com.example_flink.apitest.sinktest

import com.example_flink.domain.SensorReading
import javafx.beans.property.SimpleStringProperty
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.core.fs.{FSDataOutputStream, Path}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSinkTest {

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

//        dataStream.writeAsCsv("/out.txt")

        //final Path basePath, final BulkWriter.Factory<IN> writerFactory
        dataStream.addSink(
            StreamingFileSink.forRowFormat(
                new Path("D:\\workSpace\\Idea\\FlinkTutorial\\src\\main\\resources\\out2.txt"),
                new SimpleStringEncoder[SensorReading]()
                /*new BulkWriter.Factory[SensorReading] {
                    override def create(out: FSDataOutputStream): BulkWriter[SensorReading] = {
                        val a  = new BulkWriter[SensorReading] {
                            override def addElement(element: SensorReading): Unit = ???

                            override def flush(): Unit = ???

                            override def finish(): Unit = ???
                        }
                        a
                    }
                }*/
            ).build()
        )
//        dataStream.addSink(
//            StreamingFileSink.forRowFormat(
//                new Path("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\out1.txt"),
//                new SimpleStringEncoder[SensorReading]()
//            ).build()
//        )

        env.execute("file sink test")
    }

}
