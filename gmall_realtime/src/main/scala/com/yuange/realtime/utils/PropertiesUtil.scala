package com.yuange.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @作者：袁哥
 * @时间：2021/7/6 17:29
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}
