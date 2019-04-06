package com.wulingqi.news.util;

import com.wulingqi.news.vo.FeedsSupport;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-06
 * Time: 16:06
 */
public class TopNAlogrithm {

    /**
     *
     * 计算 rowkey
     *
     */

    public static String getKeyByFeeds(List<String> feeds, String nid) {
        Map<String, Integer> feedsSupport = FeedsSupport.getMap();
        // 3位组合，000，001，010，011，100，101，110，111
        int pre = 1;
        for (String s :
                feeds) {
            pre += Math.pow(2, feedsSupport.get(s));
        }
        return pre + "&" + nid;
    }

}
