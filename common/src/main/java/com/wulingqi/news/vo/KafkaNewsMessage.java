package com.wulingqi.news.vo;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-03
 * Time: 16:22
 */
public class KafkaNewsMessage {

    private String nid;

    private String title;

    private String author;

    private LocalDateTime date;

    private String text;

    private List<String> feeds;

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public String getNid() {
        return nid;
    }

    public void setNid(String nid) {
        this.nid = nid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public List<String> getFeeds() {
        return feeds;
    }

    public void setFeeds(List<String> feeds) {

        this.feeds = feeds.stream().sorted().collect(Collectors.toList());
    }
}
