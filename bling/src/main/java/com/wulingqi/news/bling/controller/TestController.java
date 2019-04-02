package com.wulingqi.news.bling.controller;

import com.wulingqi.news.bling.core.TestKafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("test")
public class TestController {

    private static Logger logger = LoggerFactory.getLogger(TestController.class);

    @Autowired
    private TestKafka testKafka;

    @GetMapping("kafka")
    public void testKafka() {
        logger.info("getMapping begin");
        testKafka.testKafka();
        logger.info("kafka test over");
    }
}
