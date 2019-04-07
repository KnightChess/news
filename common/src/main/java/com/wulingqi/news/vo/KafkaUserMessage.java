package com.wulingqi.news.vo;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-04
 * Time: 09:52
 */
public class KafkaUserMessage {

    private String uid;

    private String nid;

    private LocalDateTime date;

    private List<String> feeds;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getNid() {
        return nid;
    }

    public void setNid(String nid) {
        this.nid = nid;
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public List<String> getFeeds() {
        return feeds;
    }

    public void setFeeds(List<String> feeds) {
        this.feeds = feeds;
    }
}
