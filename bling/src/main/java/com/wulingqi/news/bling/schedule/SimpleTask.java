package com.wulingqi.news.bling.schedule;

import com.wulingqi.news.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 */
@Component
public class SimpleTask {

    private static Logger logger = LoggerFactory.getLogger(SimpleTask.class);

    @Scheduled(cron = "0 0 0 * * ?")
    public void reflushWeek() {
        logger.info("begin set DayOfWeek");
        DateUtil.setDayOfWeek(LocalDate.now().getDayOfWeek());
    }

    @Scheduled(cron = "0 0 0 * * ?")
    public void reflushFilPreDate() {
        logger.info("begin set Str Date");
        DateUtil.setStrDate(LocalDate.now());
    }

    //TODO 每两天定时清理redis数据(日期前缀)

}
