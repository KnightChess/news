package com.wulingqi.news.bling.schedule;

import com.wulingqi.news.util.DateUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-06
 * Time: 21:00
 */
@Component
public class SimpleTask {

    @Scheduled(cron = "0 0 0 * * ?")
    public void reflushWeek() {
        DateUtil.setDayOfWeek(LocalDate.now().getDayOfWeek());
    }

    @Scheduled(cron = "0 0 0 * * ?")
    public void reflushFilPreDate() {
        DateUtil.setStrDate(LocalDate.now());
    }

    //TODO 每两天定时清理redis数据(日期前缀)

}
