package com.wulingqi.news.bling.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-04-07
 * @time 21:00
 */
@RestController
@RequestMapping("new")
public class UserController {
    private Logger logger = LoggerFactory.getLogger(UserController.class);

    //TODO 热点数据入redis
}
