package com.yuange.gmall.gmall_publisher.mapper;

import com.yuange.gmall.gmall_publisher.beans.DAUData;
import com.yuange.gmall.gmall_publisher.beans.GMVData;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @作者：袁哥
 * @时间：2021/7/6 23:16
 */
@Repository
public interface PublisherMapper {

    //新增(当日)日活","value":1200
    Integer getDAUByDate(String date);

    //"新增设备(日活)","value":233
    Integer getNewMidCountByDate(String date);

    //"yesterday":{"11":383,"12":123,"17":88,"19":200   "11":383 封装为Bean
    List<DAUData> getDAUDatasByDate(String date);

    //查询每天的总交易额
    Double getGMVByDate(String date);

    //查询分时交易额
    List<GMVData> getGMVDatasByDate(String date);

}
