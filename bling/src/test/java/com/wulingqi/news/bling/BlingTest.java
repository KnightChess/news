package com.wulingqi.news.bling;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.bling.schedule.Redis2Hbase;
import com.wulingqi.news.vo.UserMoudle;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-04
 * Time: 13:57
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class BlingTest {

    private Logger logger = LoggerFactory.getLogger(BlingTest.class);

    @Autowired
    Redis2Hbase redis2Hbase;

    @Test
    public void testRedis2Hbase() {
//        redis2Hbase.setSomeTestData();
        redis2Hbase.init();
        redis2Hbase.testPitch();
        redis2Hbase.test();
    }

    @Test
    public void test() {
        List<UserMoudle> list = new ArrayList<>();
        UserMoudle userMoudle1 = new UserMoudle();
        userMoudle1.setHot(2);
        List<String> feeds1 = new ArrayList<>();
        feeds1.add("a");
        feeds1.add("b");
        userMoudle1.setFeeds(feeds1);
        UserMoudle userMoudle2 = new UserMoudle();
        userMoudle2.setHot(3);
        List<String> feeds2 = new ArrayList<>();
        feeds2.add("aa");
        feeds2.add("bb");
        userMoudle2.setFeeds(feeds2);
        list.add(userMoudle1);
        list.add(userMoudle2);

        String re = JSON.toJSONString(list);
        logger.info(re);

        List<UserMoudle> list1 = JSONArray.parseArray(re).toJavaList(UserMoudle.class);
        logger.info(JSON.toJSONString(list1));
    }
}
