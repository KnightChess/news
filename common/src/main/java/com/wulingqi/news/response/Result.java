package com.wulingqi.news.response;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-05-09
 * @time 17:50
 */
public class Result<T> implements Serializable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private T data;

    private int code;

    private String message;

    private String stackTrace;

    public Result() {
        this.code = ResultCode.SUCCESS;
    }

    public Result(T data) {
        this.data = data;
        this.code = ResultCode.SUCCESS;
    }

    public Result(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public Result(T data, int code, String message) {
        this.data = data;
        this.code = code;
        this.message = message;
    }

    public Result(int code, String message, String stackTrace) {
        this.code = code;
        this.message = message;
        this.stackTrace = stackTrace;
    }

    public Result<T> check() {
        if (!isSuccess()) {
            logger.error("Call Remote Failure:\n" + getMessage() + "\n" + getStackTrace());
            throw new RuntimeException(getMessage());
        }
        return this;
    }

    public boolean isSuccess(){
        return this.code == ResultCode.SUCCESS;
    }

    public T get(){
        return this.getData();
    }

    public String message(){
        return this.message;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static <T> Result<T> success(T t){
        return new Result(t);
    }

    public static <T> Result<T> success(){
        return new Result();
    }

    public static <T> Result<T> fail(String message){
        return new Result(ResultCode.ERROR, message);
    }

    public static <T> Result<T> fail(String message, Exception e){
        return new Result<>(ResultCode.ERROR, message, ExceptionUtils.getStackTrace(e));
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Result{" +
                "data=" + data +
                ", code=" + code +
                ", message='" + message + '\'' +
                '}';
    }

}
