package com.yuange.gmall.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @作者：袁哥
 * @时间：2021/7/15 10:46
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Stat {
    String title;
    List<Option> options;
}
