package com.barabanov.specific.features.kafka;

import com.barabanov.specific.features.Car;
import com.barabanov.specific.features.CarHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.barabanov.specific.features.kafka.KafkaConfiguration.CONTAINER_POST_PROCESSOR_COMMON_ERROR_HANDLER_BEAN_NAME;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaConsumer {

    private final CarHandler carHandler;


    @KafkaListener(topics = "car-topic",
            properties = "spring.json.value.default.type=com.barabanov.specific.features.Car",
            containerPostProcessor = CONTAINER_POST_PROCESSOR_COMMON_ERROR_HANDLER_BEAN_NAME)
    public void listenMsg(Car carMsg) {
        log.info("Получена машина: {}", carMsg);
        carHandler.handlerCar(carMsg);
    }

}
