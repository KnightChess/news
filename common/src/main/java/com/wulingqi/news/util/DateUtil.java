package com.wulingqi.news.util;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wulingqi
 * Date: 2019-04-06
 * Time: 20:58
 */
public class DateUtil {

    private static DayOfWeek dayOfWeek;

    private static LocalDate strDate;

    public static DayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public static void setDayOfWeek(DayOfWeek dayOfWeek) {
        DateUtil.dayOfWeek = dayOfWeek;
    }

    public static LocalDate getStrDate() {
        return strDate;
    }

    public static void setStrDate(LocalDate strDate) {
        DateUtil.strDate = strDate;
    }

    public static int getStrDayOfWeek() {
        return dayOfWeek.getValue();
    }

    public static String getStrDateForFile() {
        return strDate.format(DateTimeFormatter.ofPattern("uuuu-MM-d"));
    }

    public static void main(String[] args) {
        LocalDate date = LocalDate.now();
        System.out.println(date.format(DateTimeFormatter.ofPattern("uuuu-MM-d")));
    }
}
