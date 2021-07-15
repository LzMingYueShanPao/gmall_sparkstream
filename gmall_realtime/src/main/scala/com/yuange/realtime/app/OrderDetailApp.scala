package com.yuange.realtime.app

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import com.yuange.constants.Constants
import com.yuange.realtime.beans.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.yuange.realtime.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @作者：袁哥
 * @时间：2021/7/11 21:59
 */
object OrderDetailApp extends  BaseApp {
  override var appName: String = "OrderDetailApp"
  override var duration: Int = 10

  def main(args: Array[String]): Unit = {

    run{
      val ds1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_ORDER_INFO, streamingContext)
      val ds2: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_ORDER_DETAIL, streamingContext)

      //封装为K-V DS
      val ds3: DStream[(String, OrderInfo)] = ds1.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 封装create_date 和 create_hour   "create_time":"2021-07-07 01:39:33"
        val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time, formatter1)
        orderInfo.create_date = localDateTime.format(formatter2)
        orderInfo.create_hour = localDateTime.getHour.toString

        // 订单的明细数据，脱敏  演示手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")
        (orderInfo.id, orderInfo)
      })

      // ds3.print()
      val ds4: DStream[(String, OrderDetail)] = ds2.map(record => {
        val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (detail.order_id, detail)
      })

      // ds4.print()
      val ds5: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds3.fullOuterJoin(ds4)
      ds5.print()

      val ds6: DStream[SaleDetail] = ds5.mapPartitions(partition => {
        //存放封装后的订单详请
        val saleDetails: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

        //获取redis连接
        val jedis: Jedis = RedisUtil.getJedisClient()
        val gson = new Gson
        partition.foreach {
          case (order_id, (orderInfoOption, orderDetailOption)) => {
            if (orderInfoOption != None) {
              val orderInfo: OrderInfo = orderInfoOption.get
              // 在当前批次关联Join上的orderDetail
              if (orderDetailOption != None) {
                val orderDetail: OrderDetail = orderDetailOption.get
                val detail = new SaleDetail(orderInfo, orderDetail)
                saleDetails.append(detail)
              }

              //将order_info写入redis  ,在redis中存多久：  取系统的最大延迟（假设5min） * 2
              //  set + expire = setex
              jedis.setex("order_info:" + order_id, 5 * 2 * 60, gson.toJson(orderInfo))

              // 从redis中查询，是否有早到的order_detail
              val earlyOrderDetatils: util.Set[String] = jedis.smembers("order_detail:" + order_id)
              earlyOrderDetatils.forEach(
                str => {
                  val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
                  val detail = new SaleDetail(orderInfo, orderDetail)
                  saleDetails.append(detail)
                }
              )

            } else {
              //都是当前批次无法配对的orderDetail
              val orderDetail: OrderDetail = orderDetailOption.get

              // 从redis中查询是否有早到的order_info
              val orderInfoStr: String = jedis.get("order_info:" + orderDetail.order_id)
              if (orderInfoStr != null) {
                val detail = new SaleDetail(JSON.parseObject(orderInfoStr, classOf[OrderInfo]), orderDetail)
                saleDetails.append(detail)
              } else {
                //说明当前Order_detail 早来了，缓存中找不到对于的Order_info，需要将当前的order-detail写入redis
                jedis.sadd("order_detail:" + orderDetail.order_id, gson.toJson(orderDetail))
                jedis.expire("order_detail:" + orderDetail.order_id, 5 * 2 * 60)
              }
            }
          }
        }

        jedis.close()
        saleDetails.iterator
      })

      // 根据user_id查询 用户的其他信息
      val ds7: DStream[SaleDetail] = ds6.mapPartitions(partition => {
        val jedis: Jedis = RedisUtil.getJedisClient()
        val saleDetailsWithUserInfo: Iterator[SaleDetail] = partition.map(saleDetail => {
          val userInfoStr: String = jedis.get("user_id:" + saleDetail.user_id)
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          saleDetail
        })
        jedis.close()

        saleDetailsWithUserInfo
      })

      //写入ES   将DS转换为 docList: List[(String, Any)]
      val ds8: DStream[(String, SaleDetail)] = ds7.map(saleDetail => ((saleDetail.order_detail_id, saleDetail)))

      ds8.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          MyEsUtil.insertBulk("gmall_sale_detail" + LocalDate.now() , partition.toList)
        })

      })

    }

  }
}
