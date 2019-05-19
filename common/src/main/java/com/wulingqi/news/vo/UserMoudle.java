package com.wulingqi.news.vo;

import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
public class UserMoudle {

    private String uid;

    private List<String> feeds;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public List<String> getFeeds() {
        return feeds;
    }

    public void setFeeds(List<String> feeds) {
        this.feeds = feeds;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
