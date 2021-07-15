package com.yuange.gmall.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @作者：袁哥
 * @时间：2021/7/15 10:51
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SaleDetail {
    private String order_detail_id;
    private String order_id;
    private String order_status;
    private String create_time;
    private String user_id;
    private String sku_id;
    private String user_gender;
    private Integer user_age;
    private String user_level;
    private Double sku_price;
    private String sku_name;
    private String dt;
    // 多添加
    private String  es_metadata_id;
}
