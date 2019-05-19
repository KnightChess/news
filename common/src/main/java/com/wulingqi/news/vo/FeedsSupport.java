package com.wulingqi.news.vo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
public class FeedsSupport {

    private static Map<String, Integer> map = new HashMap<>();

    static {
        map.put("contry", 0);
        map.put("fun", 1);
        map.put("academic", 2);
    }

    public static Map<String, Integer> getMap() {
        return map;
    }

}
