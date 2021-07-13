package com.yuange.realtime.utils

import java.util.Objects
import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConverters._

/**
 * @作者：袁哥
 * @时间：2021/7/9 21:16
 */
object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
   * 获取客户端
   *
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  /*
      批量插入数据到ES

        需要先将写入的数据，封装为 docList: List[(String, Any)]
          (String, Any)：   K：id
                            V: document
   */
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

    if (docList.size > 0) {

      val jest: JestClient = getClient

      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()

      var items: util.List[BulkResult#BulkResultItem] = null

      try {
        items = jest.execute(bulk).getItems
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        //自动关闭连接
        close(jest)
        if (items!= null){
          println("保存" + items.size() + "条数据")
        }
        /*
            items: 是一个java的集合
             <- 只能用来遍历scala的集合

             将items，由Java的集合转换为scala的集合    java集合.asScala

             由scala集合转java集合 scala集合.asJava
         */
        for (item <- items.asScala) {
          if (item.error != null && item.error.nonEmpty) {
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }

}
