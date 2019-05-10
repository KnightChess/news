package com.wulingqi.news.bling.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.Context;
import com.wulingqi.news.util.RedisUtil;
import com.wulingqi.news.util.TopNAlogrithm;
import com.wulingqi.news.vo.HotNewsMessage;
import com.wulingqi.news.vo.KafkaNewsMessage;
import com.wulingqi.news.vo.NewsIndexMessage;
import com.wulingqi.news.vo.UserMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-07
 * Time: 14:12
 */
@Service
public class ServiceCore {

    private Logger logger = LoggerFactory.getLogger(ServiceCore.class);

    Connection connection = null;
    private static String zookeeperQuorum = "xxxx,aaaa,kkkk";

    /**
     * 注册
     *
     * @param userMessage
     * @return
     */
    public com.wulingqi.news.response.Result register(UserMessage userMessage) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.USER_TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(userMessage.getUid()));
            put.addColumn(Context.USER_TABLE_FA, Context.USER_TABLE_FA_DATA, Bytes.toBytes(JSONObject.toJSONString(userMessage)));
            table.put(put);
            return com.wulingqi.news.response.Result.success("register succeed");
        } catch (Exception e) {
            logger.error("can't register");
            return com.wulingqi.news.response.Result.fail("register false");
        }
    }

    /**
     * 根据用户id返回用户信息
     *
     * @param id 用户id
     * @return 用户信息
     */
    public UserMessage getUserMessageById(String id) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        UserMessage userMessage = null;
        try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.USER_TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(id));
            Result result = table.get(get);
            if (result.isEmpty()) {
                return null;
            }
            List<Cell> cells = result.getColumnCells(Context.USER_TABLE_FA, Context.USER_TABLE_FA_DATA);
            Cell cell = null;
            if (!cells.isEmpty()) {
                cell = cells.get(0);
                String message = Arrays.toString(CellUtil.cloneValue(cell));
                userMessage = JSONObject.parseObject(message, UserMessage.class);
            }
            return userMessage;

        } catch (Exception e) {
            logger.error("get userMessage error", e);
            return null;
        }
    }

    /**
     * 根据用户 id 读取 redis 中的画像，如果没有就从 hbase 中拉取，并存到 redis 中
     *
     * @param uid 用户id
     * @return 用户的feeds画像
     */
    public List<String> getUserFeedsById(String uid) {
        Jedis jedis = RedisUtil.getJedisFromPool();
        try {
            if (jedis.exists(uid + ":feedSet")) {
                SortingParams params = new SortingParams()
                        .by(uid + ":*")
                        .limit(0, 2)
                        .desc();
                List<String> feeds = jedis.sort(uid + ":feedSet", params);
                return feeds;
            } else {
                Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
                configuration.set("hbase.zookeeper.property.clientPort", "2181");

                Table table = getConnection(configuration).getTable(TableName.valueOf(Context.USER_FEED_TABLE_NAME));
                Get get = new Get(Bytes.toBytes(uid));
                Result result = table.get(get);
                List<Cell> cells = result.getColumnCells(Context.USER_FEED_TABLE_FA, Context.USER_FEED_TABLE_FA_DATA);
                if (cells.isEmpty()) {
                    //TODO 如果是null，说明没有用户的实时画像，用热点推荐
                    return null;
                }
                Cell cell = cells.get(0);
                String message = Arrays.toString(CellUtil.cloneValue(cell));
                List<String> feeds = JSONArray.parseArray(message, String.class);
                jedis.sadd(uid + ":feedSet", feeds.toArray(new String[0]));
                return feeds;
            }
        } catch (Exception e) {
            logger.error("there is something wrong in get feed from hbase or redis");
            return null;
        } finally {
            jedis.close();
        }
    }

    /**
     * 分页查询返回索引推荐
     *
     * @param feeds 用户的爱好标签
     * @param startKey hbase的分页开始Key，如果是第一次就是feeds计算的pre，如果不是，则是上一个分页的结束
     * @param pageSize 一次分页的大小
     * @return
     */
    public List<NewsIndexMessage> getNewsIndexByFeeds(List<String> feeds, String startKey, Integer pageSize) {
        // 第一次的话，startKey 为 空字符串，用feeds计算第一个位置，接下来的前端都要传最后一个startKey
        boolean firstTime = false;
        // ++1，多读取一个，作为下次的startKey
        ++pageSize;
        if (StringUtils.isBlank(startKey)) {
            startKey = TopNAlogrithm.calculatePre(feeds);
            firstTime = true;
            ++pageSize;
        }
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.NEWS_TABLE_INDEX_NAME))) {
            Filter filter = new PageFilter(pageSize);
            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.setStartRow(Bytes.toBytes(startKey));
            ResultScanner resultScanner = table.getScanner(scan);
            Iterator<Result> resultIterable = resultScanner.iterator();
            List<NewsIndexMessage> list = new ArrayList<>();
            while(resultIterable.hasNext()) {
                if (firstTime) {
                    resultIterable.next();
                    firstTime = false;
                    continue;
                }
                NewsIndexMessage newsIndexMessage = JSONObject.parseObject(resultIterable.next().getValue(
                        Context.NEWS_TABLE_INDEX_FA,
                        Context.NEWS_TABLE_INDEX_FA_DATA), NewsIndexMessage.class);
                list.add(newsIndexMessage);
            }
            return list;
        } catch (Exception e) {
            logger.error("get News Index By feeds error");
            return null;
        }
    }

    //TODO 获取热点新闻索引并写入Redis
    //TODO hot_new表加个starKey为"0000"的

    /**
     * 获取热点新闻索引并写入Redis
     *
     * @param startKey
     * @param pageSize
     * @return
     */
    public List<HotNewsMessage> getHotNewsIndexByPage(String startKey, Integer pageSize) {
        boolean firstTime = false;
        ++pageSize;
        if (StringUtils.isBlank(startKey)) {
            startKey = "0000";
            firstTime = true;
            ++pageSize;
        }
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.NEWS_TABLE_INDEX_NAME));
                Jedis jedis = RedisUtil.getJedisFromPool()) {
            Filter filter = new PageFilter(pageSize);
            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.setStartRow(Bytes.toBytes(startKey));
            ResultScanner resultScanner = table.getScanner(scan);
            Iterator<Result> resultIterable = resultScanner.iterator();
            List<HotNewsMessage> list = new ArrayList<>();
            while(resultIterable.hasNext()) {
                if (firstTime) {
                    resultIterable.next();
                    firstTime = false;
                    continue;
                }
                HotNewsMessage hotNewsMessage = JSONObject.parseObject(resultIterable.next().getValue(
                        Context.HOT_NEWS_TABLE_FA,
                        Context.HOT_NEWS_TABLE_FA_DATA), HotNewsMessage.class);
                list.add(hotNewsMessage);
            }
            logger.info("load into redis");
            jedis.sadd("hotNewsSet", list.toArray(new String[0]));
            String key = null;
            for (HotNewsMessage message :
                    list) {
                key = "hot:" + message.getNid();
                if (!jedis.exists(key)) {
                    jedis.set(key, String.valueOf(message.getClicks()));
                }
            }
            return list;
        } catch (Exception e) {
            logger.error("get News Index By feeds error");
            return null;
        }
    }

    /**
     * 根据 nid 查询用户做的选择
     *
     * @param nid 新闻id
     * @return 用户需要的新闻
     */
    public KafkaNewsMessage getNewByNid(String nid) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try (Table table = getConnection(configuration).getTable(TableName.valueOf(Context.NEWS_TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(nid));
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Context.NEWS_TABLE_FA, Context.NEWS_TABLE_FA_DATA);
            if (cells.isEmpty()) {
                logger.info("没有 {} 的详细内容，新闻不存在", nid);
                return null;
            }
            Cell cell = cells.get(0);
            String message = Arrays.toString(CellUtil.cloneValue(cell));
            return JSONObject.parseObject(message, KafkaNewsMessage.class);
        } catch (Exception e) {
            logger.error("can't get detail new to server");
            return null;
        }
    }

    /**
     * 共用connection采用hbase连接池创建
     *
     * @param configuration
     * @return
     * @throws IOException
     */
    private Connection getConnection(Configuration configuration) throws IOException {
        if (connection == null || connection.isAborted() || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }
}
