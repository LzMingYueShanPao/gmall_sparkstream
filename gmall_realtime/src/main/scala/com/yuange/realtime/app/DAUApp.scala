package com.yuange.realtime.app

import java.lang
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.yuange.constants.Constants
import com.yuange.realtime.beans.StartUpLog
import com.yuange.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
 * @作者：袁哥
 * @时间：2021/7/6 17:52
 */
object DAUApp extends BaseApp {
  override var appName: String = "DAUApp"
  override var duration: Int = 10

  def main(args: Array[String]): Unit = {
    run{
      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_STARTUP_LOG, streamingContext)
      // 从kafka中消费数据，将ConsumerRecord的value中的数据封装为 需要的Bean
      val ds1: DStream[StartUpLog] = ds.map(record => {
        //调用JSON工具，将JSON str转为 JavaBean
        val startUpLog: StartUpLog = JSON.parseObject(record.value(),classOf[StartUpLog])
        //封装 logDate，logHour
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(startUpLog.ts),ZoneId.of("Asia/Shanghai"))

        startUpLog.logDate = localDateTime.format(formatter)
        startUpLog.logHour = localDateTime.getHour.toString
        startUpLog
      })
      ds1.count().print()

      //在本批次内进行去重，取时间戳最早的那条启动日志的明细信息
      val ds3: DStream[StartUpLog] = removeDuplicateInBatch(ds1)
      ds3.count().print()

      //连接redis查询，看哪些 mid今日已经记录过了，对记录过的进行过滤
      val ds4: DStream[StartUpLog] = removeDuplicateMidsFromRedis2(ds3)
      ds4.cache()
      ds4.count().print()

      //将需要写入Hbase的 mid的信息，写入redis
      ds4.foreachRDD(rdd => {
        //以分区为单位写出
        rdd.foreachPartition(partition =>{
          //连接redis
          val jedis: Jedis = RedisUtil.getJedisClient()
          //写入到redis的  set集合中
          partition.foreach(log =>  jedis.sadd("DAU:" + log.logDate , log.mid))
          //关闭
          jedis.close()
        })
      })

      /**
       * 将明细信息写入hbase
       *    def saveToPhoenix(
       *        tableName: String,  //表名
       *        cols: Seq[String]   //RDD中的数据要写到表的哪些列,
       *        conf: Configuration = new Configuration  //hadoop包下的Configuration，不能new ,必须使用HBase提供的API创建
       *                                                 //HBaseConfiguration.create()，会 new Configuration()，再添加Hbase配置文件的信息
       *        zkUrl: Option[String] = None        //和命令行的客户端的zkUrl一致
       *        tenantId: Option[String] = None
       *     )
       * */

      ds4.foreachRDD(foreachFunc = rdd => {
        // RDD 隐式转换为 ProductRDDFunctions，再调用saveToPhoenix
        rdd saveToPhoenix(
          "GMALL_STARTUP_LOG",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop103:2181")
        )
      })
    }
  }

  /**
   * 在本批次内进行去重，取时间戳最早的那条启动日志的明细信息
   * 逻辑： 先按照mid 和日期 分组，按照ts进行排序，之后最小的
   * */
  def removeDuplicateInBatch(ds1: DStream[StartUpLog]): DStream[StartUpLog] = {
    //按照mid 和日期 分组  groupByKey
    val ds2: DStream[((String, String), StartUpLog)] = ds1.map(log => ((log.mid,log.logDate),log))
    val ds3: DStream[((String,String),Iterable[StartUpLog])] = ds2.groupByKey()

    val result: DStream[StartUpLog] = ds3.flatMap{
      case ((min,logDate),logs) => {
        val firstStartLog: List[StartUpLog] = logs.toList.sortBy(_.ts).take(1)
        firstStartLog
      }
    }
    result
  }

  /**
   * 查询Redis中，当天已经有哪些mid，已经写入到hbase,已经写入的过滤掉
   * 在Spark中进行数据库读写，都一般是以分区为单位获取连接！
   * DS中有1000条数据，2个分区，创建2个连接，发送1000次sismember请求，关闭2个连接。
   * */
  def removeDuplicateMidsFromRedis2(ds3: DStream[StartUpLog]): DStream[StartUpLog] = {
    ds3.mapPartitions(partition => {
      //连接redis
      val jedis: Jedis = RedisUtil.getJedisClient()
      //对分区数据的处理，在处理时，一个分区都共用一个连接
      val filterdLogs: Iterator[StartUpLog] = partition.filter(log => {
        //判断一个元素是否在set集合中
        val ifExists: lang.Boolean = jedis.sismember("DAU:" + log.logDate, log.mid)
        //filter算子，只留下为true的部分
        !ifExists
      })
      //关闭
      jedis.close()
      filterdLogs
    })

  }
}
