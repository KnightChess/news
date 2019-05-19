package com.wulingqi.news.util;

import com.wulingqi.news.vo.FeedsSupport;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
public class TopNAlogrithm {

    /**
     *
     * 计算new Index rowkey : feeds & timestamp & nid
     *
     */

    public static String calculatePre(List<String> feeds) {
        Map<String, Integer> feedsSupport = FeedsSupport.getMap();
        // 3位组合，000，001，010，011，100，101，110，111

        int pre = 0;
        for (String s :
                feeds) {
            pre += Math.pow(2, feedsSupport.get(s));
        }
        return String.valueOf(pre);
    }

    public static String getKeyByFeeds(List<String> feeds, String nid, LocalDateTime time) {

        return calculatePre(feeds)
                + "&" + time.format(DateTimeFormatter.ofPattern("yyyyMMddHH").withLocale(Locale.CHINA).withZone(ZoneId.systemDefault()))
                + "&" + nid;
    }

}
