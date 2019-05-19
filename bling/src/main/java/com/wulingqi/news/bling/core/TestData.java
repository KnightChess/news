package com.wulingqi.news.bling.core;

import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.vo.*;
import org.apache.commons.codec.digest.DigestUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestData {
    public static void main(String[] args) {
        String user1Uid = DigestUtils.md5Hex("wlqfzs@163.com");
        String user1Email = "wlqfzs@163.com";
        String password = DigestUtils.md5Hex("123456");

        UserMessage userMessageWulingq = new UserMessage();
        userMessageWulingq.setEmail(user1Email);
        userMessageWulingq.setPassword(password);
        userMessageWulingq.setUid(user1Uid);

        System.out.println(JSONObject.toJSONString(userMessageWulingq));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");

        List<String> wulingFeeds = new ArrayList<>();
        wulingFeeds.add("academic");
        wulingFeeds.add("fun");
        System.out.println(JSONObject.toJSONString(wulingFeeds));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");

        NewsIndexMessage newsIndexMessage = new NewsIndexMessage();
        newsIndexMessage.setAuthor("wulingqi");
        newsIndexMessage.setDate(LocalDateTime.now());
        newsIndexMessage.setNid(DigestUtils.md5Hex("news2"));
        newsIndexMessage.setTitle("news2");
        // 存入hbase 索引nid前面加入 "3|"
        System.out.println(JSONObject.toJSONString(newsIndexMessage));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
        HotNewsMessage hotNewsMessage = new HotNewsMessage();
        hotNewsMessage.setNid(DigestUtils.md5Hex("news2"));
        hotNewsMessage.setClicks(101L);
        System.out.println(JSONObject.toJSONString(hotNewsMessage));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
        KafkaNewsMessage kafkaNewsMessage = new KafkaNewsMessage();
        kafkaNewsMessage.setAuthor(newsIndexMessage.getAuthor());
        kafkaNewsMessage.setDate(newsIndexMessage.getDate());
        kafkaNewsMessage.setFeeds(Arrays.asList("academic", "fun"));
        kafkaNewsMessage.setNid(newsIndexMessage.getNid());
        kafkaNewsMessage.setText("hello world");
        kafkaNewsMessage.setTitle("WORLD");
        System.out.println(JSONObject.toJSONString(kafkaNewsMessage));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
        KafkaUserMessage kafkaUserMessage = new KafkaUserMessage();
        kafkaUserMessage.setDate(LocalDateTime.now());
        kafkaUserMessage.setFeeds(Arrays.asList("contry", "fun"));
        kafkaUserMessage.setNid(kafkaNewsMessage.getNid());
        kafkaUserMessage.setUid(user1Uid);
        System.out.println(JSONObject.toJSONString(kafkaUserMessage));
    }
}
