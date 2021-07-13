package com.yuange.realtime.app

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.yuange.constants.Constants
import com.yuange.realtime.beans.OrderInfo
import com.yuange.realtime.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

/**
 * @作者：袁哥
 * @时间：2021/7/7 20:45
 */
object GMVApp extends BaseApp {
  override var appName: String = "GMVApp"
  override var duration: Int = 10

  def main(args: Array[String]): Unit = {

    run{
      val ds: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_ORDER_INFO,streamingContext)
      //将kafka中的数据封装为样例类
      val ds1: DStream[OrderInfo] = ds.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])
        // 封装create_date 和 create_hour   "create_time":"2021-07-07 01:39:33"
        val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time,formatter1)

        orderInfo.create_date = localDateTime.format(formatter2)
        orderInfo.create_hour = localDateTime.getHour.toString

        // 订单的明细数据，脱敏  演示手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")
        orderInfo
      })

      //写入hbase
      ds1.foreachRDD(rdd => {
        rdd.saveToPhoenix(
          "GMALL_ORDER_INFO",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          HBaseConfiguration.create(),
          Some("hadoop103:2181")
        )
      })
    }
  }
}
