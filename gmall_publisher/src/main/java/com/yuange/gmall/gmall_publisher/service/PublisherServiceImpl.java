package com.yuange.gmall.gmall_publisher.service;

import com.alibaba.fastjson.JSONObject;
import com.yuange.gmall.gmall_publisher.beans.DAUData;
import com.yuange.gmall.gmall_publisher.beans.GMVData;
import com.yuange.gmall.gmall_publisher.dao.ESDao;
import com.yuange.gmall.gmall_publisher.mapper.PublisherMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @作者：袁哥
 * @时间：2021/7/6 23:15
 */
@Service
public class PublisherServiceImpl implements  PublisherService{

    @Autowired
    private PublisherMapper publisherMapper;

    @Override
    public Integer getDAUByDate(String date) {
        return publisherMapper.getDAUByDate(date);
    }

    @Override
    public Integer getNewMidCountByDate(String date) {
        Integer newMidCountByDate = publisherMapper.getNewMidCountByDate(date);
        System.out.println(newMidCountByDate);
//        return publisherMapper.getNewMidCountByDate(date);
        return newMidCountByDate;
    }

    @Override
    public List<DAUData> getDAUDatasByDate(String date) {
        List<DAUData> dauDatasByDate = publisherMapper.getDAUDatasByDate(date);
        System.out.println(dauDatasByDate);
        return dauDatasByDate;
    }

    @Override
    public Double getGMVByDate(String date) {
        return publisherMapper.getGMVByDate(date);
    }

    @Override
    public List<GMVData> getGMVDatasByDate(String date) {
        return publisherMapper.getGMVDatasByDate(date);
    }

    @Autowired
    private ESDao esDao;

    @Override
    public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {
        return esDao.getESData(date,startpage,size,keyword);
    }
}
