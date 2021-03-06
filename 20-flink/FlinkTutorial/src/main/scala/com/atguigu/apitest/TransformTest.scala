package com.atguigu.apitest

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/17 15:23
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStreamFromFile: DataStream[String] = env.readTextFile("D:\\codeMy\\CODY_MY_AFTER__KE\\sggBigData\\20-flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 1. 基本转换操作
    val dataStream: DataStream[SensorReading] = inputStreamFromFile
      .map( data => {
        // 首先是，切分
        val dataArray = data.split(",")
        // 相当于直接new的对象
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
      } )

    // 2. 分组滚动聚合 只有在keyBy之后才可以做分组的滚动聚合的操作的
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(0)
//    .keyBy("id")
//    .keyBy( data => data.id )
//    .keyBy( new MyIDSelector() )
//    .min("temperature")    // 取当前sensor的最小温度值 这个取最小值都是只是字段会变其它的都是第一个的值 sum也是可以的
//    .reduce(new MyReduce)
      .reduce( (curRes, newData) =>
      SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
    )    // 聚合出每个sensor的最大时间戳和最小温度值

    // 3.  分流
    val splitStream: SplitStream[SensorReading] = dataStream
      .split( data => {
        if( data.temperature > 30 )
          Seq("high")
        else
          Seq("low")
      } )
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

//    dataStream.print()
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")

    // 4. 合流
    val warningStream: DataStream[(String, Double)] = highTempStream.map(
      data => (data.id, data.temperature)
//      new MyMapper
    )
    // 泛型是二元组+Double
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream
      .connect(lowTempStream)
    val resultStream: DataStream[Object] = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowTempData => (lowTempData.id, "normal")
    )

    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream, allTempStream)

    resultStream.print("result")

    env.execute("transform test job")
  }
}

// 自定义函数类，key选择器 泛型前面的类型是input的类型 后i面是返回的key的类型
class MyIDSelector() extends KeySelector[SensorReading, String]{
  // 里面必须要实现一个getKey的方法
  override def getKey(value: SensorReading): String = value.id
}

// 自定义函数类 ReduceFunction
class MyRepuce() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading( value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature) )
  }
}

// 自定义MapFunction
class MyMapper extends MapFunction[SensorReading, (String, Double)]{
  override def map(value: SensorReading): (String, Double) = (value.id, value.temperature)
}

class MyRichMapper extends RichMapFunction[SensorReading, Int]{
  // 创建富函数调用的初始化的方法
  override def open(parameters: Configuration): Unit = {}

  // 获取运行时上下文 获取分区子任务的编号
  getRuntimeContext.getIndexOfThisSubtask

  override def map(value: SensorReading): Int = value.timestamp.toInt

  // 函数关闭的调用方法
  override def close(): Unit = {}
}