package com.yuange.gmall.main;

import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @作者：袁哥
 * @时间：2021/6/30 12:53
 * 负责上传数据到springboot 开发的webapp
 * 启动日志：  "type":"startup"
 *         {"area":"beijing","uid":"392","os":"ios","ch":"appstore","appid":"gmall2019","mid":"mid_23","type":"startup","vs":"1.1.1"}
 * 事件日志：  "type":"event"   "evid":"coupon"：领券
 *         {"area":"beijing","uid":"392","itemid":37,"npgid":32,"evid":"coupon","os":"ios","pgid":7,"appid":"gmall2019","mid":"mid_23","type":"event"}
 */
public class LogUploader {

    public static void sendLogStream(String log){
        try{
            //不同的日志类型对应不同的URL
            //访问nginx,由nginx将url反向代理到  hadoop102:8089/gmall_logger/log 或 hadoop103:8089/gmall_logger/log 或 hadoop104:8089/gmall_logger/log
            //访问hadoop102:81时，端口号和主机名会自动替换为 hadoop102:8089，hadoop103:8089 或 hadoop104:8089
            URL url  =new URL("http://hadoop102:81/gmall_logger/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");

            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为JSON
            //模拟使用表单发送
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();

            //logString是参数名
            out.write(("logString="+log).getBytes());
            out.flush();
            out.close();
            //获取响应码，如果是200，代表ok，否则都是发送失败
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
