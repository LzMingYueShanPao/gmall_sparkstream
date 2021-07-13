package com.yuange.realtime.app

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.JSON
import com.yuange.constants.Constants
import com.yuange.realtime.beans.{CouponAlertInfo, EventLog}
import com.yuange.realtime.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks._

/**
 * @作者：袁哥
 * @时间：2021/7/9 19:41
 */
object AlertApp extends BaseApp {
  override var appName: String = "AlertApp"
  override var duration: Int = 10

  def main(args: Array[String]): Unit = {
    run{
      val ds: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_EVENT_LOG,streamingContext)

      //封装样例类
      val ds1: DStream[EventLog] = ds.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
//        println(eventLog.toString)

        //根据ts 为logDate 和 logHour赋值
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventLog.ts), ZoneId.of("Asia/Shanghai"))

        eventLog.logDate = localDateTime.format(formatter)
        eventLog.logHour = localDateTime.getHour.toString

        eventLog
      })

      //开窗：采集过去5分钟的数据
      //然后统计每个设备，每个用户登录时的行为：将数据变为K-V结构，然后按Key分组
      val ds2: DStream[((String,String),Iterable[(EventLog)])]  = ds1.window(Minutes(5)).map(log => {
        ((log.mid, log.uid), log)
      }).groupByKey()

      //判断在5分钟内，用户所产生的行为是否需要预警，将需要预警的数据留下
      //数据包括：设备id，用户id，5分钟内产生的日志
      val ds3: DStream[(String,Iterable[EventLog])] = ds2.map {
        case ((mid, uid), logs) => {
          //是否要预警的标记，默认不需要预警
          var ifneedAlert: Boolean = false
          breakable {
            logs.foreach(log => {
              //只要浏览了商品，就不预警
              if ("clickItem".equals(log.evid)) {
                ifneedAlert = false
                break()
              } else if ("coupon".equals(log.evid)) { //领取了优惠券，符合预警条件
                ifneedAlert = true
              }
            })
          }

          if (ifneedAlert) {
            (mid, logs)
          } else {
            (null, null)
          }
        }
      }

      //过滤空值
      val ds4: DStream[(String,Iterable[EventLog])] = ds3.filter(_._1 != null)

      //按照设备id聚合，并将每个设备中登录用户数小于3个的过滤掉,最后将value进行扁平化处理
      val ds5: DStream[(String,Iterable[EventLog])] = ds4.groupByKey().filter(_._2.size >= 3).mapValues(_.flatten)

      //生成预警日志
      val ds6: DStream[CouponAlertInfo] = ds5.map {
        case (mid, logs) => {
          var uids: util.HashSet[String] = new java.util.HashSet[String]()
          var itemIds: util.HashSet[String] = new java.util.HashSet[String]()
          var events: util.List[String] = new util.ArrayList[String]()

          logs.foreach(log => {
            uids.add(log.uid)
            events.add(log.evid)

            if ("coupon".equals(log.evid)) {
              itemIds.add(log.itemid)
            }
          })
          //将数据进行封装
          CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
        }
      }

      //将DS中的数据，转换为docList: List[(String, Any)]，再写入，确保mid唯一
      val ds7: DStream[(String,CouponAlertInfo)] = ds6.map(info => {
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(info.ts), ZoneId.of("Asia/Shanghai"))

        (localDateTime.format(formatter) + "_" + info.mid, info)
      })

      //写入ES
      ds7.foreachRDD(rdd => {
        //以分区为单位写入，节省创建连接的开销
        rdd.foreachPartition(partition => {
          //将这个分区的数据，封装为 docList: List[(String, Any)]
          val list: List[(String,CouponAlertInfo)] = partition.toList

          MyEsUtil.insertBulk("gmall_coupon_alert"+LocalDate.now() , list)
        })
      })
    }
  }
}
