package com.wulingqi.news.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-04-07
 * @time 15:16
 */
public class RedisUtil {

    private static JedisPool pool = null;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMinIdle(0);
        pool = new JedisPool(jedisPoolConfig, "localhost", 6379, 3000);
    }

    public static Jedis getJedisFromPool() {
        return pool.getResource();
    }

}
