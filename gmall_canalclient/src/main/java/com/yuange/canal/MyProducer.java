package com.yuange.canal;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @作者：袁哥
 * @时间：2021/7/7 18:54
 */
public class MyProducer {

    private static Producer<String,String> producer;

    static {
        producer=getProducer();
    }

    // 提供方法返回生产者
    public static Producer<String,String> getProducer(){
        Properties properties = new Properties();
        //参考 ProducerConfig
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);
    }

    //发送数据至kafka
    public static void sendDataToKafka(String topic,String msg){
        producer.send(new ProducerRecord<String, String>(topic,msg));
    }

}
