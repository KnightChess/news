package com.wulingqi.news.bling.controller;

import com.wulingqi.news.bling.core.AsyncSimpleIFileAppendService;
import com.wulingqi.news.bling.core.ServiceCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("news")
public class TestController {

    private static Logger logger = LoggerFactory.getLogger(TestController.class);


    @Autowired
    ServiceCore serviceCore;
    @Autowired
    AsyncSimpleIFileAppendService asyncSimpleIFileAppendService;



}
