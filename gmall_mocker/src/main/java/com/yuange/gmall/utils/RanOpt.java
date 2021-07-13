package com.yuange.gmall.utils;

/**
 * @作者：袁哥
 * @时间：2021/6/30 12:51
 * 为value赋值一个weight
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
