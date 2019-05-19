package com.wulingqi.news.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
public class RedisUtil {

    private static JedisPool pool = null;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMinIdle(0);
        pool = new JedisPool(jedisPoolConfig, "slave5", 6379, 3000);
    }

    public static Jedis getJedisFromPool() {
        return pool.getResource();
    }

}
