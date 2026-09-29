package com.kafkaorder.consumer_notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ConsumerNotificationApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerNotificationApplication.class, args);
	}

}
