package com.wulingqi.news.request;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-05-09
 * @time 19:55
 */
public class RecommendCondition {

    private String uid;

    private String hotNewsStartKey;

    private String newsStartKey;

    private Integer pageSize = 10;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getHotNewsStartKey() {
        return hotNewsStartKey;
    }

    public void setHotNewsStartKey(String hotNewsStartKey) {
        this.hotNewsStartKey = hotNewsStartKey;
    }

    public String getNewsStartKey() {
        return newsStartKey;
    }

    public void setNewsStartKey(String newsStartKey) {
        this.newsStartKey = newsStartKey;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}
