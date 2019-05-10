package com.wulingqi.news.bling.controller;

import com.wulingqi.news.bling.core.AsyncSimpleIFileAppendService;
import com.wulingqi.news.bling.core.ServiceCore;
import com.wulingqi.news.request.RecommendCondition;
import com.wulingqi.news.response.Result;
import com.wulingqi.news.vo.HotNewsMessage;
import com.wulingqi.news.vo.KafkaNewsMessage;
import com.wulingqi.news.vo.NewsIndexMessage;
import com.wulingqi.news.vo.UserMessage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-04-07
 * @time 21:00
 */
@RestController
@RequestMapping("news")
public class UserController {
    private Logger logger = LoggerFactory.getLogger(UserController.class);

    @Autowired
    ServiceCore serviceCore;
    @Autowired
    AsyncSimpleIFileAppendService asyncSimpleIFileAppendService;

    //TODO 用户注册
    @PostMapping("/register")
    public Result userRegister(@RequestBody UserMessage userMessage) {
        logger.info("begin register");
        Result result  = serviceCore.register(userMessage);
        return result;
    }
    //TODO 用户登录，然后判断是否冷启动操作，冷启动纯热点推荐，非冷启动混合推荐，返回两种推荐的EndKey
    //TODO token?
    @PostMapping("/loginInit")
    public Result userLoginAndInit(@RequestParam String uid,
                                   @RequestParam String password,
                                   @RequestParam(defaultValue = "10") Integer pageSize) {
        if (StringUtils.isBlank(uid) || StringUtils.isBlank(password)) {
            logger.info("uid or password can't be null");
            return Result.fail("uid or password can't be null");
        }
        UserMessage userMessage = serviceCore.getUserMessageById(uid);
        if (userMessage == null || !password.equals(userMessage.getPassword())) {
            logger.info("用户名或者密码错误");
            return Result.fail("用户名或者密码错误");
        }
        // 获取用户 feeds 画像
        List<String> feeds = serviceCore.getUserFeedsById(uid);
        if (feeds == null || feeds.isEmpty()) {
            // 热点推荐
            logger.info("冷启动");
            List<HotNewsMessage> hotNewsList = serviceCore.getHotNewsIndexByPage("0000", pageSize);
            return Result.success(hotNewsList);
        } else {
            // 混合推荐
            List<HotNewsMessage> hotNewsList = serviceCore.getHotNewsIndexByPage("0000", pageSize/2);
            List<NewsIndexMessage> newsIndexList = serviceCore.getNewsIndexByFeeds(feeds, "0", (pageSize + 1)/2);
            List<Object> all = new ArrayList<>(hotNewsList.size() + newsIndexList.size() + 4);
            all.addAll(newsIndexList);
            all.addAll(hotNewsList);
            return Result.success(all);
        }
    }
    //TODO 下一页推荐（混合推荐）
    @PostMapping("/recommend/mixTure")
    public Result mixTurePageRecommend(@RequestBody RecommendCondition condition) {
        if (StringUtils.isBlank(condition.getHotNewsStartKey()) || StringUtils.isBlank(condition.getNewsStartKey())) {
            logger.info("mixTureRecommend must has two startKey");
            return Result.fail("mixTureRecommend must has two startKey");
        }
        if (StringUtils.isBlank(condition.getUid())) {
            logger.info("uid must not be null");
            return Result.fail("uid must not be null");
        }
        // get user feeds
        List<String> feeds = serviceCore.getUserFeedsById(condition.getUid());
        // get news index
        List<HotNewsMessage> hotNewsMessages = serviceCore.getHotNewsIndexByPage(condition.getHotNewsStartKey(), condition.getPageSize()/2);
        List<NewsIndexMessage> newsIndexMessages = serviceCore.getNewsIndexByFeeds(
                feeds, condition.getNewsStartKey(), (condition.getPageSize() + 1)/2);

        List<Object> all = new ArrayList<>(hotNewsMessages.size() + newsIndexMessages.size() + 4);
        all.addAll(newsIndexMessages);
        all.addAll(hotNewsMessages);

        return Result.success(all);
    }

    //TODO 下一页推荐（个性化推荐）
    @PostMapping("/recommend/special")
    public Result getPageSpecialRecommand(@RequestBody RecommendCondition condition) {
        if (StringUtils.isBlank(condition.getNewsStartKey())) {
            logger.info("newsStartKey must have");
            return Result.fail("newsStartKey must have");
        }
        if (StringUtils.isBlank(condition.getUid())) {
            logger.info("uid must not be null");
            return Result.fail("uid must not be null");
        }
        // get user feeds
        List<String> feeds = serviceCore.getUserFeedsById(condition.getUid());
        List<NewsIndexMessage> newsIndexMessages = serviceCore.getNewsIndexByFeeds(
                feeds, condition.getNewsStartKey(), condition.getPageSize());
        return Result.success(newsIndexMessages);
    }

    //TODO 下一页推荐（热点推荐）
    @PostMapping("/recommend/hot")
    public Result getPageHotNews(@RequestBody RecommendCondition condition) {
        if (StringUtils.isBlank(condition.getHotNewsStartKey())) {
            logger.info("hotNewsStartKey must have");
            return Result.fail("hotNewsStartKey must have");
        }
        if (StringUtils.isBlank(condition.getUid())) {
            logger.info("uid must not be null");
            return Result.fail("uid must not be null");
        }
        List<HotNewsMessage> hotNewsMessageList = serviceCore.getHotNewsIndexByPage(condition.getHotNewsStartKey(), condition.getPageSize());
        return Result.success(hotNewsMessageList);
    }

    //TODO 选择某个新闻，返回详细信息
    @GetMapping("/detail")
    public Result getNewsDetail(@RequestParam String nid, @RequestParam String uid) {
        if (StringUtils.isBlank(nid)) {
            logger.info("nid can't be null");
            return Result.fail("nid can't be null");
        }
//        KafkaNewsMessage kafkaNewsMessage = serviceCore.getNewByNid(nid);
        KafkaNewsMessage kafkaNewsMessage = new KafkaNewsMessage();
        kafkaNewsMessage.setNid("dsfasfd2");
        //TODO 行为日志写入filebeat搜集的本地
        asyncSimpleIFileAppendService.appendAction(kafkaNewsMessage, uid);
        return Result.success(kafkaNewsMessage);
    }
}
