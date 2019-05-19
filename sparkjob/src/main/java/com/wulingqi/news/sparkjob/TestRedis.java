package com.wulingqi.news.sparkjob;

import com.alibaba.fastjson.JSON;
import com.wulingqi.news.vo.KafkaUserMessage;

import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
public class TestRedis {

    public static void main(String[] args) {
//        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//        jedisPoolConfig.setMaxIdle(8);
//        jedisPoolConfig.setMaxTotal(8);
//        jedisPoolConfig.setMinIdle(0);
//        JedisPool pool = new JedisPool(jedisPoolConfig, "172.18.18.248", 16379, 3000);
//        try (Jedis jedis = pool.getResource()) {
//            jedis.set("wulingqi", "123");
//        }
        KafkaUserMessage kafkaUserMessage = new KafkaUserMessage();
        kafkaUserMessage.setUid("213213");
        kafkaUserMessage.setNid("{\"date\":\"2019-05-07T18:10:33.856\",\"feeds\":[\"apple\",\"pear\"],\"nid\":\"dsfasfd2\",\"uid\":\"2132131\"}\n");
        kafkaUserMessage.setDate(LocalDateTime.now());
        kafkaUserMessage.setFeeds(Arrays.asList("apple", "banana"));
        System.out.println(JSON.toJSONString(kafkaUserMessage));
    }
}
