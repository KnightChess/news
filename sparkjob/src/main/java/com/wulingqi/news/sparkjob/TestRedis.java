package com.wulingqi.news.sparkjob;

import com.alibaba.fastjson.JSON;
import com.wulingqi.news.vo.KafkaUserMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-05-07
 * @time 14:57
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
        kafkaUserMessage.setNid("dsfasfd");
        kafkaUserMessage.setDate(LocalDateTime.now());
        kafkaUserMessage.setFeeds(Arrays.asList("apple", "banana"));
        System.out.println(JSON.toJSONString(kafkaUserMessage));
    }
}
