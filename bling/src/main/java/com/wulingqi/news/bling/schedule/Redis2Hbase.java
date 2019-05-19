package com.wulingqi.news.bling.schedule;

import com.alibaba.fastjson.JSON;
import com.wulingqi.news.Context;
import com.wulingqi.news.vo.HotNewsMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
@Component
public class Redis2Hbase {

    private Logger logger = LoggerFactory.getLogger(Redis2Hbase.class);

    private static JedisPool pool = null;
    private static Connection connection = null;
    private static String zookeeperQuorum = "master,slave3,slave4";

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMinIdle(0);
        pool = new JedisPool(jedisPoolConfig, "slave5", 6379, 3000);
    }

    //TODO 60分钟入录一次
    /**
     * 用户画像入库
     *
     * @throws Exception
     */
    @Scheduled(cron = "0 * * * * ?")
    public void userMoudleRedis2Hbase() {
        logger.info("begin load userMoudle to hbase");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Map<String, List<String>> result = new HashMap<>();
        try (Jedis jedis = pool.getResource()) {
            Set<String> users = jedis.smembers("uidSet");
            for (String user :
                    users) {
                SortingParams params = new SortingParams()
                        .by(user + ":*")
                        .limit(0,2)
                        .desc();
                List<String> feeds = jedis.sort(user + ":feedSet", params);
                result.put(user, feeds);
            }
        } catch (Exception e) {
            logger.error("there is some wrong in get userMoudle from hbase");
            throw e;
        }

        // 开始写入本地缓存
        logger.info("build put list");
        try(Table table = getConnection(configuration).getTable(TableName.valueOf(Context.USER_FEED_TABLE_NAME))) {
            List<Put> list = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry :
                    result.entrySet()) {
                if (entry.getValue().size() < 1) continue;
                Put put = new Put(Bytes.toBytes(entry.getKey()));
                put.addColumn(Context.USER_FEED_TABLE_FA, Context.USER_FEED_TABLE_FA_DATA, Bytes.toBytes(JSON.toJSONString(entry.getValue())));
                list.add(put);
            }
            // 写入hbase
            logger.info("write to hbase");
            table.put(list);
        } catch (Exception e) {
            logger.error("write to hbase error", e);
        }
    }

    //TODO 热点新闻入录hbase，也是一天一次
    @Scheduled(cron = "0 0 0 * * ?")
    public void hotNewsRedis2Hbase() {
        logger.info("begin load hotNews to hbase");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try (Jedis jedis = pool.getResource()) {
            logger.info("begin get from redis");
            Set<String> hotNewsSet = jedis.smembers("hotNewsSet");
            List<String> keyList = hotNewsSet.stream().map(x -> "hot:" + x).collect(Collectors.toList());
            List<String> valueList = jedis.mget(keyList.toArray(new String[0]));

            HotNewsMessage hotNewsMessage = new HotNewsMessage();
            List<Put> putList = new ArrayList<>();
            Iterator<String> keyIterator = keyList.iterator();
            Iterator<String> valueIterator = valueList.iterator();
            String key = null;
            long click;
            while (keyIterator.hasNext() && valueIterator.hasNext()) {
                key = keyIterator.next();
                click = Long.valueOf(valueIterator.next());
                if (click < 100) continue;
                hotNewsMessage.setNid(key);
                hotNewsMessage.setClicks(click);
                Put put = new Put(Bytes.toBytes(key));
                put.addColumn(Context.HOT_NEWS_TABLE_FA, Context.HOT_NEWS_TABLE_FA_DATA, Bytes.toBytes(JSON.toJSONString(hotNewsMessage)));
                putList.add(put);
            }
            try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.HOT_NEWS_TABLE_NAME))) {
                table.put(putList);
            } catch (Exception e) {
                logger.error("write to hbase error", e);
            }
        }
    }

    public static Connection getConnection(Configuration configuration) throws IOException {
        if (connection == null || connection.isClosed() || connection.isAborted()) {
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    public void testPitch() {

        try (Jedis jedis = pool.getResource()) {
            logger.info("begin get from redis");
            String cursor = "0";
            ScanResult<String> result;
            ScanParams params = new ScanParams();
            params.match("*zzz*");
            params.count(5);
            do {
                result = jedis.scan(cursor, params);
                cursor = result.getStringCursor();
                logger.info("curor {}", result.getStringCursor());
                String[] arryResult = result.getResult().toArray(new String[0]);
                if (arryResult.length > 0) {
                    List<String> values = jedis.mget(arryResult);
                    System.out.println(values.toString());
                }
            } while ("0".compareTo(result.getStringCursor()) != 0);
        }
    }

    public void test() {
        DayOfWeek dayOfWeek = LocalDate.now().getDayOfWeek();
        System.out.println(dayOfWeek.getValue());
        try (Jedis jedis = pool.getResource()) {
            logger.info("sort begin");
            Long preTime = System.nanoTime();
            List<String> users = jedis.lrange("uidList", 0, -1);
            int n = 1000;
            while (n-- > 0) {
                for (String user :
                        users) {
                    SortingParams params = new SortingParams()
                            .by(user + ":*")
                            .limit(0, 3)
                            .desc();
                    List<String> re = jedis.sort("uid:feedList", params);
                }
            }
            Long afterTime = System.nanoTime();
            System.out.println("cost : " + (afterTime - preTime));
        }
    }
}
