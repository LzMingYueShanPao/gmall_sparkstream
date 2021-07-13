package com.yuange.realtime.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @作者：袁哥
 * @时间：2021/7/6 17:32
 *     使用的是Jedis提供的连接池,会出现问题：
 *            比如一个线程使用redis，发送一个命令，之后将连接放回池中，
 *            第二个线程，从池中借走了这个连接，连接的socket的buffer不会清空，会由上一个线程发送的残留数据
 */
object RedisUtil {

  val config: Properties = PropertiesUtil.load("config.properties")
  val host: String = config.getProperty("redis.host")
  val port: String = config.getProperty("redis.port")

  //不使用连接池
  def getJedisClient():Jedis={
    new Jedis(host,port.toInt)
  }
}
