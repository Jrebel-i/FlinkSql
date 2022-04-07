package com.hc.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestKafkaLog4gAppender {
    // 初始化logger
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaLog4gAppender.class);

    public static void main(String[] args) throws IOException {

        int start = 0;
        while (true) {
            // 使用logger来生产数据
            logger.warn("string-" + start);
            System.out.println("string-" + start);
            start++;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
