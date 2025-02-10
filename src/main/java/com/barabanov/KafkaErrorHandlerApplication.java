package com.barabanov;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class KafkaErrorHandlerApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaErrorHandlerApplication.class, args);
        KafkaTemplate<String, Object> kafkaTemplate = context.getBean(KafkaTemplate.class);
        ThreadLocalRandom currentRandom = ThreadLocalRandom.current();

//		for (int i = 0; i < 10; i++) {
//			kafkaTemplate.send("my-topic", new Lada("model-" + currentRandom.nextInt(),
//					Instant.now()));
//		}

    }

}
