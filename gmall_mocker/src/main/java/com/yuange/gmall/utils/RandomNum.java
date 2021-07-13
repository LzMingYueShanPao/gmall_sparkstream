package com.yuange.gmall.utils;

import java.util.Random;

/**
 * @作者：袁哥
 * @时间：2021/6/30 12:52
 * 产生一个从  [fromNum,toNum]的随机整数
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}