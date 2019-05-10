package com.wulingqi.news.bling.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@Service
public class TestKafka {

    private static Logger logger = LoggerFactory.getLogger(TestKafka.class);

    @Value("${kafka.zookeeper.quorum}")
    private String quorum;
}
