package com.wulingqi.news.bling.sparkJob;

import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.vo.KafkaUserMessage;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-03
 * Time: 18:40
 */
public class Kafka2Redis {

    private static Logger logger = LoggerFactory.getLogger(Kafka2Hbase.class);

    private static JedisPool pool;

    // 1, 日志地址
    // 2,
    private static String zookeeperQuorum = "xxxx,aaaa,kkkk";
    private static String kafkaBroker = "slave3:9092,slave4:9092,slave5:9092";
    private static Connection connection = null;

    public static void main(String[] args) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMinIdle(0);
        pool = new JedisPool(jedisPoolConfig, "localhost", 6379, 3000);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBroker);
    }

    public static void doJob(Map<String, String> kafkaParams, Set<String> kafkaTopics) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkStreamingToHbase");
        Duration duration = Durations.seconds(5L);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
        JavaPairDStream<String, String> dStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                kafkaTopics);

        logger.info("clean kafka streaming");
        JavaDStream<String> messageStream = dStream.map(Tuple2::_2).map(row -> {
            JSONObject jsonObject = JSONObject.parseObject(row);
            String messageValue = jsonObject.getString("message");
            return messageValue;
        }).cache();

        logger.info("every rdd message to hbase");
        messageStream.foreachRDD(rdd -> rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Redis::writeToRedis));

        try {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            logger.info("there is something wrong in the awaitTermination");
            e.printStackTrace();
        }
    }

    public static void writeToRedis(Iterator<String> stringIterator) {
        try (Jedis jedis = pool.getResource()) {
            // 行为日志写入redis，包括hotNewsSet和用户feed的增加
            while(stringIterator.hasNext()) {
                KafkaUserMessage kafkaUserMessage = JSONObject.toJavaObject(JSONObject.parseObject(stringIterator.next()), KafkaUserMessage.class);
                // 所有用户近期访问的新闻数据
                jedis.sadd("hotNewsSet", kafkaUserMessage.getNid());
                // 所有近期新闻数据的点击量+1
                jedis.incr("hot:" + kafkaUserMessage.getNid());
                // 近期登陆的所有用户
                jedis.sadd("uidSet", kafkaUserMessage.getUid());
                // 某用户的兴趣分类
                jedis.sadd(kafkaUserMessage.getUid() + ":feedSet", kafkaUserMessage.getFeeds().toArray(new String[0]));
                // 某用户的兴趣点击
                for (String feed :
                        kafkaUserMessage.getFeeds()) {
                    jedis.incr(kafkaUserMessage.getUid() + ":" + feed);
                }
            }
        }
    }

}
