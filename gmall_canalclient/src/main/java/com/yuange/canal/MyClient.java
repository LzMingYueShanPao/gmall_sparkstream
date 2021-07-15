package com.yuange.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yuange.constants.Constants;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @作者：袁哥
 * @时间：2021/7/7 18:10
 * 步骤：
 *    ①创建一个客户端：CanalConnector（SimpleCanalConnector:  单节点canal集群、ClusterCanalConnector： HA canal集群）
 *    ②使用客户端连接 canal server
 *    ③指定客户端订阅 canal server中的binlog信息
 *    ④解析binlog信息
 *    ⑤写入kafka
 *
 * 消费到的数据的结构：
 *      Message:  代表拉取的一批数据，这一批数据可能是多个SQL执行，造成的写操作变化
 *          List<Entry> entries ： 每个Entry代表一个SQL造成的写操作变化
 *          id ： -1 说明没有拉取到数据
 *      Entry：
 *          CanalEntry.Header header_ :  头信息，其中包含了对这条sql的一些说明
 *              private Object tableName_: sql操作的表名
 *              EntryType; Entry操作的类型
 *                  开启事务： 写操作  begin
 *                  提交事务： 写操作  commit
 *                  对原始数据进行影响的写操作： rowdata
 *                          update、delete、insert
 *          ByteString storeValue_：   数据
 *              序列化数据，需要使用工具类RowChange，进行转换,转换之后获取到一个RowChange对象
 */
public class MyClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        /**
         * 创建一个canal客户端
         * public static CanalConnector newSingleConnector(
         *              SocketAddress address,  //指定canal server的主机名和端口号
         *              tring destination,      //参考/opt/module/canal/conf/canal.properties中的canal.destinations 属性值
         *              String username,        //不是instance.properties中的canal.instance.dbUsername
         *              String password         //参考AdminGuide（从canal 1.1.4 之后才提供的），链接地址：https://github.com/alibaba/canal/wiki/AdminGuide
         * ) {...}
         * */
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop104", 11111),
                "example", "", "");

        //使用客户端连接 canal server
        canalConnector.connect();

        //指定客户端订阅 canal server中的binlog信息  只统计在Order_info表
        canalConnector.subscribe("gmall_realtime.*");

        //不停地拉取数据   Message[id=-1,entries=[],raw=false,rawEntries=[]] 代表当前这批没有拉取到数据
        while (true){
            Message message = canalConnector.get(100);
            //判断是否拉取到了数据，如果没有拉取到，歇一会再去拉取
            if (message.getId() == -1){
                System.out.println("暂时没有数据，先等会");
                Thread.sleep(5000);
                continue;
            }

            // 数据的处理逻辑
            List<CanalEntry.Entry> entries = message.getEntries();
            for (CanalEntry.Entry entry : entries) {
                //判断这个entry的类型是不是rowdata类型，只处理rowdata类型
                if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                    ByteString storeValue = entry.getStoreValue();          //数据
                    String tableName = entry.getHeader().getTableName();    //表名
                    handleStoreValue(storeValue,tableName);
                }
            }
        }
    }

    private static void handleStoreValue(ByteString storeValue, String tableName) throws InvalidProtocolBufferException {
        //将storeValue 转化为 RowChange
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        /**
         * 一个RowChange代表多行数据
         * order_info: 可能会执行的写操作类型，统计GMV    total_amount
         *          insert :  会-->更新后的值
         *          update ：  不会-->只允许修改 order_status
         *          delete :  不会，数据是不允许删除
         * 判断当前这批写操作产生的数据是不是insert语句产生的
         * */

        // 采集 order_info 的insert
        if ( "order_info".equals(tableName) && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)) {
            writeDataToKafka(Constants.GMALL_ORDER_INFO,rowChange);
        // 采集 order_detail 的insert
        }else if ("order_detail".equals(tableName) && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)) {
            writeDataToKafka(Constants.GMALL_ORDER_DETAIL,rowChange);
        }else if((rowChange.getEventType().equals(CanalEntry.EventType.INSERT)
                ||rowChange.getEventType().equals(CanalEntry.EventType.INSERT))&&"user_info".equals(tableName)){
            writeDataToKafka(Constants.GMALL_USER_INFO,rowChange);
        }
    }

    public static void writeDataToKafka(String topic,CanalEntry.RowChange rowChange){
        //获取行的集合
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();

            //获取insert后每一行的每一列
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(),column.getValue());
            }

            // 模拟网络波动和延迟  随机产生一个 1-5直接的随机数
            int i = new Random().nextInt(15);

            /*try {
                Thread.sleep(15 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //发送数据到Kafka
            MyProducer.sendDataToKafka(topic,jsonObject.toJSONString());
        }
    }
}
