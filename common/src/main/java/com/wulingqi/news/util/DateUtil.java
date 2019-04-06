package com.wulingqi.news.util;

import java.time.DayOfWeek;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-06
 * Time: 20:58
 */
public class DateUtil {

    private static DayOfWeek dayOfWeek;

    public static DayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public static void setDayOfWeek(DayOfWeek dayOfWeek) {
        DateUtil.dayOfWeek = dayOfWeek;
    }

    public static int getDayValue() {
        return dayOfWeek.getValue();
    }
}
