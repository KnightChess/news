package com.wulingqi.news.sparkjob;

import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.vo.KafkaUserMessage;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
public class Kafka2Redis {

    private static Logger logger = LoggerFactory.getLogger(Kafka2Hbase.class);

    private static JedisPool pool;

    // 1, 日志地址
    // 2,
//    private static String kafkaBroker = "slave3:9092,slave4:9092,slave5:9092";
    private static String kafkaBroker = "192.168.149.136:9092,192.168.149.137:9092,192.168.149.138:9092";
//    private static String kafkaBroker = "localhost:9092";

    /** zookeeper 服务地址，ip:port，多个地址用逗号隔开 */
    private static final String SERVER_ADDRESS = "192.168.149.133:2181,192.168.149.136:2181,192.168.149.137:2181";
//    private static final String SERVER_ADDRESS = "localhost:2181";
    /** 超时时间，毫秒为单位 */
    private static final Integer TIMEOUT_MS = 60000;
    /** 节点名 */
    private static final String NODE = "/mysparkjob/redis";
    private static ZooKeeper zk;

    public static void main(String[] args) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMinIdle(0);
        pool = new JedisPool(jedisPoolConfig, "192.168.149.138", 6379, 3000);

        try {
            zk = new ZooKeeper(SERVER_ADDRESS, TIMEOUT_MS, null);
            waitUntilConnected(zk);
        } catch (IOException e) {
            logger.error("zk create error", e);
        }

        doOffsetJob("UserActive");
    }

//    public static void doJob(Map<String, String> kafkaParams, Set<String> kafkaTopics) {
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("SparkStreamingToHbase");
//        Duration duration = Durations.seconds(5L);
//        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
//        JavaPairDStream<String, String> dStream = KafkaUtils.createDirectStream(
//                javaStreamingContext,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                kafkaTopics);
//
//        dStream.foreachRDD((VoidFunction<JavaPairRDD<String, String>>) stringStringJavaPairRDD -> {
//            OffsetRange[] offsetRanges = ((HasOffsetRanges) stringStringJavaPairRDD.rdd()).offsetRanges();
//        });
//
//        logger.info("clean kafka streaming");
//        JavaDStream<String> messageStream = dStream.map(Tuple2::_2).map(row -> {
//            JSONObject jsonObject = JSONObject.parseObject(row);
//            String messageValue = jsonObject.getString("message");
//            return messageValue;
//        }).cache();
//
//        logger.info("every rdd message to redis");
//        messageStream.foreachRDD(rdd -> rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Redis::writeToRedis));
//
//        try {
//            javaStreamingContext.start();
//            javaStreamingContext.awaitTermination();
//        } catch (InterruptedException e) {
//            logger.info("there is something wrong in the awaitTermination");
//            e.printStackTrace();
//        }
//    }

    private static void doOffsetJob(String kafkaTopic) {
        logger.info("begin do Offerset Job");
        long seconds = 5;
        SparkConf sparkConf = new SparkConf().setAppName("SparkStringToRedis").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(seconds));

        // kafkaParams
//        HashSet<String> topicSet = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBroker);

        Map<TopicAndPartition, Long> fromOffset = MyKafkaUtils.readOffset(kafkaTopic, zk, NODE);

//        scala.collection.immutable.Map kafkaParamsMap = (scala.collection.immutable.Map) JavaConverters.mapAsScalaMapConverter(kafkaParams).asScala();
//        KafkaCluster kc = new KafkaCluster(kafkaParamsMap);
//        kc.getPa

        logger.info("createDirectStream");
        JavaInputDStream<Map> stream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                Map.class,
                kafkaParams,
                fromOffset,
                (Function<MessageAndMetadata<String, String>, Map>) mmd -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("topicName", mmd.topic());
                    map.put("partition", mmd.partition());
                    map.put("offset", mmd.offset());
                    map.put("message", mmd.message());
                    return map;
                }
        );

        final OffsetRange[][] offsetRanges = new OffsetRange[1][1];

        stream.foreachRDD(mapJavaRDD -> offsetRanges[0] = ((HasOffsetRanges)mapJavaRDD.rdd()).offsetRanges());

        JavaDStream<String> messageStream = stream.map(row -> row.get("message").toString()).cache();

        logger.info("every rdd message to redis");
        messageStream.foreachRDD(rdd -> {
            rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Redis::writeToRedis);
            MyKafkaUtils.saveOffset(kafkaTopic, offsetRanges[0], zk, NODE);
        });


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("jssc await error", e);
        }}

    private static void writeToRedis(Iterator<String> stringIterator) {
        try (Jedis jedis = pool.getResource()) {
            // 行为日志写入redis，包括hotNewsSet和用户feed的增加
            while(stringIterator.hasNext()) {
                String string = stringIterator.next();
                logger.info("----------------String {}", string);
                JSONObject jsonObject = JSONObject.parseObject(string);
                KafkaUserMessage kafkaUserMessage = JSONObject.parseObject(jsonObject.getString("message"), KafkaUserMessage.class);
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

    /**
     * 防止java程序未连上zookeeper的服务器而往下执行，需等待zk的状态码
     * @param zooKeeper 等待的zk cli
     */
    private static void waitUntilConnected(ZooKeeper zooKeeper) {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        Watcher watcher = new ConnectedWatcher(connectedLatch);
        zooKeeper.register(watcher);
        if (ZooKeeper.States.CONNECTING == zooKeeper.getState()) {
            try {
                connectedLatch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static class ConnectedWatcher implements Watcher {
        private CountDownLatch connectedLatch;

        ConnectedWatcher(CountDownLatch connectedLatch) {
            this.connectedLatch = connectedLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        }
    }

}
