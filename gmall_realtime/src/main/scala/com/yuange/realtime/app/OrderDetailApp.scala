package com.yuange.realtime.app

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import com.yuange.constants.Constants
import com.yuange.realtime.beans.{OrderDetail, OrderInfo, SaleDetail}
import com.yuange.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @作者：袁哥
 * @时间：2021/7/11 21:59
 */
object OrderDetailApp extends BaseApp {
  override var appName: String = "OrderDetailApp"
  override var duration: Int = 10

  def main(args: Array[String]): Unit = {
    run{
      val ds1: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_ORDER_INFO, streamingContext)
      val ds2: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_ORDER_DETAIL,streamingContext)

      //封装为K-V
      val ds3: DStream[(String,OrderInfo)] = ds1.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])

        // 封装create_date 和 create_hour   "create_time":"2021-07-07 01:39:33"
        val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time,formatter1)

        orderInfo.create_date = localDateTime.format(formatter2)
        orderInfo.create_hour = localDateTime.getHour.toString

        // 订单的明细数据，脱敏  演示手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")

        (orderInfo.id,orderInfo)
      })

      val ds4: DStream[(String,OrderDetail)] = ds2.map(record => {
        val detail: OrderDetail = JSON.parseObject(record.value(),classOf[OrderDetail])
        (detail.order_id,detail)
      })

      val ds5: DStream[(String,(Option[OrderInfo],Option[OrderDetail]))] = ds3.fullOuterJoin(ds4)
      ds5.print()

      ds5.mapPartitions(partition => {
        //存放封装后的订单详请
        val saleDetail: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
        //获取redis连接
        val jedis: Jedis = RedisUtil.getJedisClient()

        val gson = new Gson()

        partition.foreach{
          case (order_id,(orderInfoOption,orderDetailOptio)) => {
            if (orderInfoOption != None){

            }
          }
        }
      })

    }
  }
}
