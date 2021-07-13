package com.yuange.realtime.beans

/**
 * @作者：袁哥
 * @时间：2021/7/9 19:38
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
