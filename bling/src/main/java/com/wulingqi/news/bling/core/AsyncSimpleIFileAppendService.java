package com.wulingqi.news.bling.core;

import com.wulingqi.news.vo.KafkaNewsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 */
@Component
public class AsyncSimpleIFileAppendService {

    private static final Logger log = LoggerFactory.getLogger(AsyncSimpleIFileAppendService.class);
    private static final int MAX_WAIT_SIZE = 10000;
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 10L, TimeUnit.HOURS, new LinkedBlockingDeque<>(1), new ThreadPoolExecutor.DiscardPolicy());

    public boolean appendAction(KafkaNewsMessage kafkaNewsMessage, String uid) {
        log.info("submint action to pools");
        this.executor.submit(() -> {
            FileBeatApi.submitUserAction(kafkaNewsMessage, uid);
        });
//        FileBeatApi.submitUserAction(kafkaNewsMessage, uid);
        return true;
    }

}
