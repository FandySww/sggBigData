package com.atguigu.apitest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

case class SensorReadingMy(id: String, timestamp: Long, temperature: Double)

object SoucetestMy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource( new MySensorSourcMy() )
    stream.print("stream")
    env.execute("stream")
  }
}


// 实现一个自定义的 SourceFunction，自动生成测试数据
class MySensorSourcMy() extends SourceFunction[SensorReading]{
  // 定义一个flag，表示数据源是否正常运行
  var running: Boolean = true
  override def cancel(): Unit = {running = false}
  // 随机生成 SensorReading数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()
    // 随机生成 10个传感器的温度值，并且不停在之前温度基础上更新（随机上下波动）
    // 首先生成 10个传感器的初始温度
    var curTemps = 1.to(10).map(
      // 二元组
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )
    // 无限循环，生成随机数据流
    while(running){
      // 在当前温度基础上，随机生成微小波动
      curTemps = curTemps.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前系统时间
      val curTs = System.currentTimeMillis()
      // 包装成样例类，用ctx发出数据 ctx搜集起来就发送到flink里面去了
      curTemps.foreach(
        data => ctx.collect(SensorReading(data._1, curTs, data._2))
      )
      // 定义间隔时间
      Thread.sleep(1000L)
    }
  }
}