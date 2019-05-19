package com.wulingqi.news.sparkjob;

import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
public class MyKafkaUtils {

    private static Logger logger = LoggerFactory.getLogger(MyKafkaUtils.class);

    public static Map<TopicAndPartition, Long> readOffset(String topicName, ZooKeeper zk, String parentPath) {
        String nodePath = parentPath + "/" + topicName;
        try {
            Stat stat = zk.exists(nodePath, null);
            if (stat == null) {
                zk.create(nodePath, topicName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            List<String> partitions = zk.getChildren(nodePath, false);
            Map<TopicAndPartition, Long> map = new HashMap<>();
            if (partitions.size() > 0) {
                for (String partition :
                        partitions) {
                    String partitionPath = nodePath + "/" + partition;
                    byte[] offset = zk.getData(nodePath + "/" + partition, null, null);
                    TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, Integer.valueOf(partition));
                    map.put(topicAndPartition, Long.valueOf(new String(offset)));
                }
            }
            return map;
        } catch (Exception e) {
            logger.error("readOffset error", e);
            return null;
        }
    }

    public static void saveOffset(String topicName, OffsetRange[] offsetRanges, ZooKeeper zk, String parentPath) {
        try {
            Stat stat = null;
            logger.info("saving offset");
            for (OffsetRange offsetRange :
                    offsetRanges) {
                String partitionPath = parentPath + "/" + topicName + "/" + offsetRange.partition();

                stat = zk.exists(partitionPath, false);

                if (stat == null) {
                    zk.create(partitionPath, String.valueOf(offsetRange.partition()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    continue;
                }
                long lastOffset = Long.valueOf(new String(zk.getData(partitionPath, null, null)));
                if (lastOffset < offsetRange.fromOffset()) {
                    zk.setData(partitionPath, String.valueOf(offsetRange.fromOffset()).getBytes(), -1);
                }
            }
            logger.info("saving offset successfully");
        } catch (Exception e) {
            logger.error("saveOffset error", e);
        }
    }

}
