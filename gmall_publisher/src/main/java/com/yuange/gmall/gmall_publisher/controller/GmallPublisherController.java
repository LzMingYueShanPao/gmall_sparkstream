package com.yuange.gmall.gmall_publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.yuange.gmall.gmall_publisher.beans.DAUData;
import com.yuange.gmall.gmall_publisher.beans.GMVData;
import com.yuange.gmall.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @作者：袁哥
 * @时间：2021/7/6 23:10
 */
@RestController
public class GmallPublisherController {
    @Autowired
    private PublisherService publisherService;

    //http://localhost:8070/realtime-total?date=2021-07-06
    @RequestMapping(value = "/realtime-total")
    public Object handle1(String date){
        ArrayList<JSONObject> result = new ArrayList<>();

        Integer dau = publisherService.getDAUByDate(date);
        Integer newMidCounts = publisherService.getNewMidCountByDate(date);
        Double gmv = publisherService.getGMVByDate(date);

        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("id","dau");
        jsonObject1.put("name","新增日活");
        jsonObject1.put("value",dau);

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("id","new_mid");
        jsonObject2.put("name","新增设备");
        jsonObject2.put("value",newMidCounts);

        JSONObject jsonObject3 = new JSONObject();
        jsonObject3.put("id","order_amount");
        jsonObject3.put("name","新增交易额");
        jsonObject3.put("value",gmv);

        result.add(jsonObject1);
        result.add(jsonObject2);
        result.add(jsonObject3);
        return result;
    }

    @RequestMapping(value = "/realtime-hours")
    public Object handle2(String id,String date){
        //根据今天求昨天的日期
        LocalDate toDay = LocalDate.parse(date);
        String yestodayDate = toDay.minusDays(1).toString();

        JSONObject result = new JSONObject();

        if ("dau".equals(id)){
            List<DAUData> todayDatas = publisherService.getDAUDatasByDate(date);
            List<DAUData> yestodayDatas = publisherService.getDAUDatasByDate(yestodayDate);

            JSONObject jsonObject1 = parseData(todayDatas);
            JSONObject jsonObject2 = parseData(yestodayDatas);

            result.put("yesterday",jsonObject2);
            result.put("today",jsonObject1);
        }else{
            List<GMVData> todayDatas = publisherService.getGMVDatasByDate(date);
            List<GMVData> yestodayDatas = publisherService.getGMVDatasByDate(yestodayDate);

            JSONObject jsonObject1 = parseGMVData(todayDatas);
            JSONObject jsonObject2 = parseGMVData(yestodayDatas);

            result.put("yesterday",jsonObject2);
            result.put("today",jsonObject1);
        }
        return result;
    }

    public JSONObject parseGMVData(List<GMVData> datas){
        JSONObject jsonObject = new JSONObject();
        for (GMVData data : datas) {
            jsonObject.put(data.getHour(),data.getAmount());
        }
        return jsonObject;
    }

    //负责把　List<DAUData> 　封装为一个JSONObject
    public JSONObject parseData(List<DAUData> datas){
        JSONObject jsonObject = new JSONObject();
        for (DAUData data : datas) {
            jsonObject.put(data.getHour(),data.getNum());
        }
        return jsonObject;
    }

}
