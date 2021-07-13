package com.yuange.gmall.utils;

import java.util.Date;
import java.util.Random;

/**
 * @作者：袁哥
 * @时间：2021/6/30 12:50
 * 产生随机日期
 */
public class RandomDate {

    Long logDateTime =0L;
    int maxTimeStep=0 ;

    public RandomDate (Date startDate , Date  endDate,int num) {
        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();
    }

    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }
}
