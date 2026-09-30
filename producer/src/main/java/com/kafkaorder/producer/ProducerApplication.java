package com.kafkaorder.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// 1 - Inicia a aplicação;
// 2 - Cria o contexto do Spring;
// 3 - Encontra os componentes;
// 4 - Configura o servidor HTTP;
// 5 - Sobe a aplicação.

@SpringBootApplication
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

}
