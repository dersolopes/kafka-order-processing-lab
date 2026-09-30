package com.kafkaorder.consumer_audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ConsumerAuditApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerAuditApplication.class, args);
	}

}
