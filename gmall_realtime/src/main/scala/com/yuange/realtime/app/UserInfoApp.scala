package com.yuange.realtime.app

import com.alibaba.fastjson.JSON
import com.yuange.constants.Constants
import com.yuange.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream

/**
 * @作者：袁哥
 * @时间：2021/7/13 20:11
 */
object UserInfoApp extends BaseApp {
  override var appName: String = "UserInfoApp"
  override var duration: Int = 10
//"{\"alipay_trade_no\":\"osXI-42833805\",\"payment_type\":\"unionpay\",\"out_trade_no\":\"8245321891\",\"user_id\":\"4\",\"total_amount\":\"349.0\",\"subject\":\"RxLonxzd\",\"payment_time\":\"2021-07-07 17:59:52\",\"id\":\"16\",\"order_id\":\"27\"}"
  //写入的user info就没有 birthday这个属性   所以这个地方是null
//  val date= formattor.parse(userInfo.birthday)
//  {"birthday":"1962-12-02","login_name":"SIORVczqeydZKDJOQNtB","gender":"M","create_time":"2021-07-07 00:41:01","passwd":"pwdULUpRqdtJqyyUcFRsqyu","nick_name":"EqQgjChghsrIpAIOSaxn","head_img":"http://taXMPOBnLTjqZZbhJWFnyDyFEhZBHHnukQxRRIPb","name":"DJxuHYZZYUFoEfPIVFmxycQWmsdmcy","user_level":"4","phone_num":"13284737360","id":"16","email":"DCdKonzJ@LJo.com"}
//  很奇怪你的kafka中的数据读出来和写入到redis的不一样
  def main(args: Array[String]): Unit = {
    run{
      val ds: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(Constants.GMALL_USER_INFO,streamingContext)
      ds.foreachRDD(rdd => {
        rdd.foreachPartition(partiton => {
          //获取连接
          val jedis = RedisUtil.getJedisClient()
          partiton.foreach(record => {
            val key: String = JSON.parseObject(record.value()).getString("id")
            jedis.set("user_id:" + key, record.value())
            println(record.value())
          })
          jedis.close()
        })
      })
    }
  }
}
