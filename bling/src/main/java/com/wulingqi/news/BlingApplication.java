package com.wulingqi.news;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BlingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BlingApplication.class, args);
    }
}
