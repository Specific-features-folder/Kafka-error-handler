package com.barabanov.specific.features.kafka;

import com.barabanov.specific.features.Car;
import com.barabanov.specific.features.CarHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaConsumer {

    private final CarHandler carHandler;


    @KafkaListener(topics = "car-topic",
            properties = "spring.json.value.default.type=com.barabanov.specific.features.Car")
    public void listenCarMsg(Car carMsg) {
        log.info("Получена машина: {}", carMsg);
        carHandler.handlerCar(carMsg);
    }


}
