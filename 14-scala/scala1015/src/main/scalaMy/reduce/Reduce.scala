package reduce


import java.lang

import org.apache.flink.api.common.functions.{GroupReduceFunction, MapFunction, ReduceFunction, RichGroupReduceFunction, RichReduceFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 * @Author fandayong
 * @Date 2021/2/10 9:54 AM
 * @description
 */
// 网址：https://blog.csdn.net/dinghua_xuexi/article/details/107766222
// reduce的map:
object Reduce {
  def main(args: Array[String]): Unit = {
    //    val env = ExecutionEnvironment.getExecutionEnvironment
    //    val path = this.getClass.getResource ("/data").getPath
    //    val text = env.readTextFile (path).setParallelism (2)
    //    val group_ds = text.flatMap (_.split (" ")).map ((_,1))
    //      .groupBy (0)
    //    group_ds.reduce((x,y) => (x._1, x._2 + y._2))
    //      .print()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
    env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L)).keyBy(0).map // 以数组的第一个元素作为key
    ((longLongTuple2: Nothing) => "key:" + longLongTuple2.f0 + ",value:" + longLongTuple2.f1.asInstanceOf[MapFunction[Nothing, String]]).print


  }
}
