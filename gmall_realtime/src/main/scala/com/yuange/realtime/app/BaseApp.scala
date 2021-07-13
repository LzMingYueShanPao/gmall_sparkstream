package com.yuange.realtime.app

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @作者：袁哥
 * @时间：2021/7/6 17:45
 *     四个需求，都需要使用SparkStreaming从Kafka消费数据，因此四个需求的开发流程是一样的
 *                ①创建一个 StreamingContext
 *                ②从kafka获取DS
 *                ③对DS进行转换：四个需求需要对DS进行不同的转换 ---->业务----->将抽取为一段功能，将功能在不同的需求中作为参数进行传入(控制抽象)
 *                ④启动App
 *                ⑤阻塞当前线程
 */
abstract class BaseApp {
  //声明appName
  var appName:String
  //声明  Duration(一批数据的采集周期)
  var duration:Int
  //不是一个抽象属性
  var streamingContext:StreamingContext=null

  //运行程序
  def run(op: => Unit) {
    try {
      streamingContext = new StreamingContext("local[*]", appName, Seconds(duration))
      //程序自定义的处理逻辑逻辑
      op
      //启动app
      streamingContext.start()
      //阻塞当前线程
      streamingContext.awaitTermination()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        streamingContext.stop(true)
    }
  }
}
