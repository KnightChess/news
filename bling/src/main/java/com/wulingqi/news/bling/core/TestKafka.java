package com.wulingqi.news.bling.core;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TestKafka {

    private static Logger logger = LoggerFactory.getLogger(TestKafka.class);

    @Value("${kafka.zookeeper.quorum}")
    private String quorum;

    public void testKafka() {
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply("192.168.149.133:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
            logger.info("zkUtils create over");
//            AdminUtils.createTopic(zkUtils, "aaa", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
//            logger.info("create topic succeed");
            boolean exists = AdminUtils.topicExists(zkUtils, "wulingqi");
//            logger.info("topic is exists {}", exists);
            AdminUtils.deleteTopic(zkUtils, "aaa");
            logger.info("delete topic succeed");
            exists = AdminUtils.topicExists(zkUtils, "aaa");
            logger.info("topic is exists {}", exists);
        } catch (Exception e) {
            logger.error("kafka ddl error", e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }
}
