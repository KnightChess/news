package com.wulingqi.news;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
public interface Context {

    // 新闻表配置
    byte[] NEWS_TABLE_NAME = Bytes.toBytes("news");
    byte[] NEWS_TABLE_FA = Bytes.toBytes("data");
    byte[] NEWS_TABLE_FA_DATA = Bytes.toBytes("message");


    // 新闻索引表配置
    byte[] NEWS_TABLE_INDEX_NAME= Bytes.toBytes("news_index");
    byte[] NEWS_TABLE_INDEX_FA = Bytes.toBytes("data");
    byte[] NEWS_TABLE_INDEX_FA_DATA = Bytes.toBytes("message");

    // 用户信息表
    byte[] USER_TABLE_NAME = Bytes.toBytes("users");
    byte[] USER_TABLE_FA = Bytes.toBytes("data");
    byte[] USER_TABLE_FA_DATA = Bytes.toBytes("message");

    // 用户画像表
    byte[] USER_FEED_TABLE_NAME = Bytes.toBytes("feeds");
    byte[] USER_FEED_TABLE_FA = Bytes.toBytes("data");
    byte[] USER_FEED_TABLE_FA_DATA = Bytes.toBytes("feeds");

    // 热点数据表
    byte[] HOT_NEWS_TABLE_NAME = Bytes.toBytes("hot_new");
    byte[] HOT_NEWS_TABLE_FA = Bytes.toBytes("data");
    byte[] HOT_NEWS_TABLE_FA_DATA = Bytes.toBytes("message");
}
