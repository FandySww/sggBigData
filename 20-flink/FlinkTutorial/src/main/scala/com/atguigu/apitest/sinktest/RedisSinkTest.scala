package com.atguigu.apitest.sinktest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/18 10:32
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("D:\\codeMy\\CODY_MY_AFTER__KE\\sggBigData\\20-flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    // 定义一个redis的配置类
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()
    // 定义一个 RedisMapper key和value是怎么实现的呢
    val myMapper = new RedisMapper[SensorReading] {
      // 定义保存数据到redis的命令，hset table_name key value
      override def getCommandDescription: RedisCommandDescription = {
        // sensor_temp 表名
        new RedisCommandDescription( RedisCommand.HSET, "sensor_temp" )
      }
      override def getValueFromData(data: SensorReading): String = data.temperature.toString
      override def getKeyFromData(data: SensorReading): String = data.id
    }
    dataStream.addSink(new RedisSink[SensorReading](conf, myMapper))
    env.execute("redis sink test")
  }
}
