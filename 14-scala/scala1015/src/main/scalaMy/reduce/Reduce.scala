package reduce



import java.lang

import org.apache.flink.api.common.functions.{GroupReduceFunction, ReduceFunction, RichGroupReduceFunction, RichReduceFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
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
  def main (args: Array[String] ): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val path = this.getClass.getResource ("/data").getPath
    val text = env.readTextFile (path).setParallelism (2)
    val group_ds = text.flatMap (_.split (" ")).map ((_,1))
      .groupBy (0)
    group_ds.reduce((x,y) => (x._1, x._2 + y._2))
      .print()
  }
}
