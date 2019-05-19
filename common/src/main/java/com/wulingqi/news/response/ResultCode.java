package com.wulingqi.news.response;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
public interface ResultCode {

    int SUCCESS = 200;

    int ERROR = 400;

    int NOT_FOUND = 404;

    int SERVICE_UNAVAILABLE = 503;

    int REDIRECT = 302;

    int WARN = 999;

}
