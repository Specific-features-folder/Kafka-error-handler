package com.barabanov.specific.features.kafka;

import com.barabanov.specific.features.DatabaseHelper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;


@Slf4j
@RequiredArgsConstructor
@Component
// автоматически внедрится в ConcurrentKafkaListenerContainerFactory, если будет обнаружен в контексте бин типа CommonErrorHandler
public class KafkaCommonErrorHandler implements CommonErrorHandler {

    private final DatabaseHelper databaseHelper;


    /**
     * При использовании старой версии spring-kafka необходимо вручную сдвигать оффсет в обработчике ошибок.
     * В новой версии АПИ void handleRecord стал boolean handleOne.
     * В качестве boolean можно возвращать необходимо ли считать эту запись обработанной и сдвинуть оффсет или нет.
     * Следовательно не требуется ручное увеличение оффсета.
     */
    @SneakyThrows
    @Override
    public void handleRecord(Exception exception,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {

        if (databaseHelper.isDatabaseAvailable()) {
            log.error("Произошла ошибка при обработке сообщения из kafka. Во время обработки ошибки есть подключение к БД. " +
                    "Offset будет увеличен! Текущие значения Topic: {} Partition: {} Offset: {}", record.topic(), record.partition(), record.offset());
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
            consumer.commitSync();
        } else
            log.error("Во время обработки ошибки при чтении из кафка подключение к БД отсутствует! Offset не будет увеличен." +
                            " Текущие значения Topic: {} Partition: {} Offset: {}",
                    record.topic(), record.partition(), record.offset());

    }


    @Override
    public void handleOtherException(Exception exception, Consumer<?, ?> consumer, MessageListenerContainer container, boolean batchListener) {
        if (exception instanceof RecordDeserializationException ex) {
            TopicPartition topicPartition = ex.topicPartition();
            log.error("Ошибка десериализации при попытке считать сообщение из kafka. Offset будет увеличен!" +
                            " Текущие значения Topic: {} Partition: {} Offset: {}",
                    topicPartition.topic(), topicPartition.partition(), ex.offset());
            consumer.seek(topicPartition, ex.offset() + 1L);
            consumer.commitSync();
        } else
            log.error("Неожиданная ошибка при попытке считать сообщение из kafka.");
    }

}