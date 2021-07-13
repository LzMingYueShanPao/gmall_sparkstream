package com.yuange.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yuange.constants.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @作者：袁哥
 * @时间：2021/6/30 10:44
 */
@Slf4j          //自动帮你创建  Logger log = Logger.getLogger(LogController.class);
@RestController //Controller+responsebody
public class LogController {

    @Autowired
    private KafkaTemplate<String,String> producer;

    @RequestMapping(value = "/log")     //value的值需要和gmall_mock中LogUploader的url对应
    public String handleLog(String logString){  //形参的属性名需要和gmall_mock中LogUploader的请求参数名一致：out.write(("logString="+log).getBytes());
        //在服务器端为日志生成时间
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        //将日志数据落盘，使用Log4j
        log.info(jsonObject.toJSONString());

        //将数据生产到kafka
        if ("event".equals(jsonObject.getString("type"))){
            producer.send(Constants.GMALL_EVENT_LOG,jsonObject.toString());
        }else {
            producer.send(Constants.GMALL_STARTUP_LOG,jsonObject.toString());
        }
        return "success";
    }
}
