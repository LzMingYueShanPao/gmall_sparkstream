package com.yuange.realtime.beans

/**
 * @作者：袁哥
 * @时间：2021/7/6 17:35
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)
