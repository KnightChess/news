package com.wulingqi.news.response;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
public class PageResult<T> extends Result<T> {

    public PageResult() {
    }

    private Long total;

    public PageResult(T data, Long total) {
        super(data);
        this.total = total;
    }

    public PageResult(int code, String message, String stackTrace) {
        super(code, message, stackTrace);
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "PageResult{" +
                "total=" + total +
                '}';
    }

}
