package com.wulingqi.news.sparkjob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.Context;
import com.wulingqi.news.util.TopNAlogrithm;
import com.wulingqi.news.vo.KafkaNewsMessage;
import com.wulingqi.news.vo.NewsIndexMessage;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

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
    private static String zookeeperQuorum = "xxxx,aaaa,kkkk";
    private static String kafkaBroker = "slave3:9092,slave4:9092,slave5:9092";
    private static Connection connection = null;

    public static void main(String[] args) {
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
        messageStream.foreachRDD(rdd -> rdd.foreachPartition((VoidFunction<Iterator<String>>) Kafka2Hbase::writeToHbase));

        try {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            logger.info("there is something wrong in the awaitTermination");
            e.printStackTrace();
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
            while(stringIterator.hasNext()) {
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
}
