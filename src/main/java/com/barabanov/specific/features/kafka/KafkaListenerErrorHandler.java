package com.barabanov.specific.features.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;


/**
 * Вызывается, когда метод с @KafkaListener выбрасывает ошибку. Аналогичный этому обработку функционал можно получить,
 * если обернуть всё тело метода с @KafkaListener в try/catch.
 */
@Slf4j
@Component(KafkaListenerErrorHandler.KAFKA_LISTENER_ERROR_HANDLER_BEAN_NAME)
public class KafkaListenerErrorHandler implements org.springframework.kafka.listener.KafkaListenerErrorHandler {

    public static final String KAFKA_LISTENER_ERROR_HANDLER_BEAN_NAME = "kafkaListenerErrorHandler";


    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {

        log.info("При обработке сообщении: {} из kafka произошла ошибка. Сообщения будут проигнорированы. StackTrace ошибки: \n{}",
                message.getPayload(), exception.getStackTrace());
        return null;
    }
}
