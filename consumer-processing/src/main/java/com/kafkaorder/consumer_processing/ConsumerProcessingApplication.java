package com.kafkaorder.consumer_processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ConsumerProcessingApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerProcessingApplication.class, args);
	}

}
