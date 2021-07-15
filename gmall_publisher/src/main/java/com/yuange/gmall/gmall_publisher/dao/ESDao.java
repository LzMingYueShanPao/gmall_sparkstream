package com.yuange.gmall.gmall_publisher.dao;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

/**
 * @作者：袁哥
 * @时间：2021/7/15 10:53
 */
public interface ESDao {

    JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException;

}
