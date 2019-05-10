package com.wulingqi.news.sparkjob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.Context;
import com.wulingqi.news.util.TopNAlogrithm;
import com.wulingqi.news.vo.KafkaNewsMessage;
import com.wulingqi.news.vo.NewsIndexMessage;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-02
 * Time: 21:11
 */
public class Kafka2Hbase {

    private static Logger logger = LoggerFactory.getLogger(Kafka2Hbase.class);

    // 1, 日志地址
    // 2,
    private static String zookeeperQuorum = "master,slave3,slave4";
    private static String kafkaBroker = "slave3:9092,slave4:9092,slave5:9092";
    private static Connection connection = null;

    /**
     * zookeeper 服务地址，ip:port，多个地址用逗号隔开
     */
    private static final String SERVER_ADDRESS = "master:2181,slave3:2181,slave4:2181";
    /**
     * 超时时间，毫秒为单位
     */
    private static final Integer TIMEOUT_MS = 3000;
    /**
     * 节点名
     */
    private static final String NODE = "/mysparkjob/hbase";
    private static ZooKeeper zk;

    public static void main(String[] args) {

        try {
            zk = new ZooKeeper(SERVER_ADDRESS, TIMEOUT_MS, null);
            waitUntilConnected(zk);
        } catch (IOException e) {
            logger.error("zk create error", e);
        }
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
//        logger.info("clean kafka streaming");
//        JavaDStream<String> messageStream = dStream.map(Tuple2::_2).map(row -> {
//            JSONObject jsonObject = JSONObject.parseObject(row);
//            String messageValue = jsonObject.getString("message");
//            return messageValue;
//        }).cache();
//
//        logger.info("every rdd message to hbase");
//        messageStream.foreachRDD(rdd -> rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Hbase::writeToHbase));
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
        long seconds = 5;
        SparkConf sparkConf = new SparkConf().setAppName("SparkStringToRedis");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(seconds));

        // kafkaParams
//        HashSet<String> topicSet = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBroker);

        Map<TopicAndPartition, Long> fromOffset = MyKafkaUtils.readOffset(kafkaTopic, zk, NODE);

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

        stream.foreachRDD(mapJavaRDD -> offsetRanges[0] = ((HasOffsetRanges) mapJavaRDD.rdd()).offsetRanges());

        JavaDStream<String> messageStream = stream.map(row -> row.get("message").toString()).cache();

        logger.info("every rdd message to hbase");
        messageStream.foreachRDD(rdd -> {
            rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Hbase::writeToHbase);
            MyKafkaUtils.saveOffset(kafkaTopic, offsetRanges[0], zk, NODE);
        });


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("jssc await error", e);
        }
    }

    public static void writeToHbase(Iterator<String> stringIterator) {

        logger.info("begin write news to hbase");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try {

            Table tableReal = getConnection(configuration).getTable(TableName.valueOf(Context.NEWS_TABLE_NAME));
            Table tableIndex = getConnection(configuration).getTable(TableName.valueOf(Context.NEWS_TABLE_INDEX_NAME));

            KafkaNewsMessage kafkaNewsMessage = null;
            NewsIndexMessage kafkaNewsIndexMessage = new NewsIndexMessage();

            List<Put> putReals = new ArrayList<>();
            List<Put> putIndexs = new ArrayList<>();
            while (stringIterator.hasNext()) {
                JSONObject object = JSONObject.parseObject(stringIterator.next());
                kafkaNewsMessage = object.toJavaObject(KafkaNewsMessage.class);

                Put putReal = new Put(Bytes.toBytes(kafkaNewsMessage.getNid()));
                putReal.addColumn(Context.NEWS_TABLE_FA, Context.NEWS_TABLE_FA_DATA, Bytes.toBytes(JSON.toJSONString(kafkaNewsMessage)));

                kafkaNewsIndexMessage.setAuthor(kafkaNewsMessage.getAuthor());
                kafkaNewsIndexMessage.setDate(kafkaNewsMessage.getDate());
                kafkaNewsIndexMessage.setNid(kafkaNewsMessage.getNid());
                kafkaNewsIndexMessage.setTitle(kafkaNewsMessage.getTitle());
                Put putIndex = new Put(Bytes.toBytes(TopNAlogrithm.getKeyByFeeds(kafkaNewsMessage.getFeeds(), kafkaNewsMessage.getNid(), kafkaNewsMessage.getDate())));
                putIndex.addColumn(Context.NEWS_TABLE_INDEX_FA, Context.NEWS_TABLE_INDEX_FA_DATA, Bytes.toBytes(JSON.toJSONString(kafkaNewsIndexMessage)));

                putReals.add(putReal);
                putIndexs.add(putIndex);
            }

            tableReal.put(putReals);
            tableIndex.put(putIndexs);
            logger.info("news write to hbase succeed");

        } catch (Exception e) {
            logger.info("write to hbase error", e);
        }
    }

    public static Connection getConnection(Configuration configuration) throws IOException {
        if (connection == null || connection.isClosed() || connection.isAborted()) {
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
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
