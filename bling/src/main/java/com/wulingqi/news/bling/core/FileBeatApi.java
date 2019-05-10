package com.wulingqi.news.bling.core;

import com.alibaba.fastjson.JSONObject;
import com.wulingqi.news.util.DateUtil;
import com.wulingqi.news.vo.KafkaNewsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created with IntelliJ IDEA.
 *
 * @author wulingqi
 * @date 2019-05-10
 * @time 11:45
 */
public class FileBeatApi {

    private static Logger logger = LoggerFactory.getLogger(FileBeatApi.class);

    private static String kafkaDataPath = "/Users/mfw/tmp/logData/";

    public static void submitUserAction(KafkaNewsMessage kafkaNewsMessage, String uid) throws IOException {
        FileOutputStream fileOutputStream = null;
        String fileDataAbsPath = kafkaDataPath + DateUtil.getStrDateForFile();
        try {
            checkFileMod(fileDataAbsPath);
            fileOutputStream = new FileOutputStream(new File(fileDataAbsPath), true);
            byte[] data = JSONObject.toJSONString(kafkaNewsMessage).getBytes(Charset.forName("utf-8"));
            fileOutputStream.write(data);
        } catch (IOException e) {
            logger.error("error", e);
        } finally {
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
        }
    }

    public static boolean checkFileMod(String logPath) throws IOException {
        File logFile = new File(logPath);
        boolean result = true;
        if (!logFile.exists() || !logFile.isFile()) {
            result |= logFile.createNewFile();
            result |= logFile.setWritable(true);
            result |= logFile.setReadable(true);
            result |= logFile.setExecutable(true);
        }
        return result;
    }

}
