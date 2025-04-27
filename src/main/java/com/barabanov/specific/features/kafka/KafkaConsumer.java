package com.barabanov.specific.features.kafka;

import com.barabanov.specific.features.Bicycle;
import com.barabanov.specific.features.Car;
import com.barabanov.specific.features.handler.BicycleHandler;
import com.barabanov.specific.features.handler.CarHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaConsumer {

    private final CarHandler carHandler;
    private final BicycleHandler bicycleHandler;


    @KafkaListener(topics = "${kafka-topics.car-topic}",
            properties = "spring.json.value.default.type=com.barabanov.specific.features.Car",
            errorHandler = KafkaListenerErrorHandler.KAFKA_LISTENER_ERROR_HANDLER_BEAN_NAME)
    public void listenCarMsg(Car carMsg) {
        log.info("Получена машина: {}", carMsg);
        carHandler.handlerCar(carMsg);
    }


    @KafkaListener(topics = "${kafka-topics.bicycle-topic}",
            properties = "spring.json.value.default.type=com.barabanov.specific.features.Bicycle",
            errorHandler = KafkaListenerErrorHandler.KAFKA_LISTENER_ERROR_HANDLER_BEAN_NAME,
            batch = "true")
    public void listenBicycleMsgBatch(List<Bicycle> bicycleBatch) {
        log.info("Получены велосипеды моделей: {}",
                bicycleBatch.stream()
                        .map(Bicycle::model)
                        .toList());

        bicycleHandler.handleBicycleBatch(bicycleBatch);
    }

}
