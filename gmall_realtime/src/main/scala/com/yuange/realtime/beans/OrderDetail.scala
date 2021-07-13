package com.yuange.realtime.beans

/**
 * @作者：袁哥
 * @时间：2021/7/11 21:34
 *   样例类的字段：
 *        如果需要取kafka的全部字段，设置对应的全部字段
 *        此外额外添加自己需要的字段,如果不需要Kafka中的全部字段，可以只取需要的字段
 */
case class OrderDetail(id:String,
                       order_id: String,
                       sku_name: String,
                       sku_id: String,
                       order_price: String,
                       img_url: String,
                       sku_num: String)
