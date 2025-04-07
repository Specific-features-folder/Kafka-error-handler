package com.barabanov.specific.features.kafka;

import com.barabanov.specific.features.service.RawMsgService;
import com.barabanov.specific.features.utils.DatabaseHelper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecoveryStrategy;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;


@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaCommonErrorHandler implements CommonErrorHandler {

    private final static long MSG_PROCESSING_ATTEMPTS = 0L;
    private final RecoveryStrategy recoveryStrategy = new FailedRecordTracker(null, new FixedBackOff(0, MSG_PROCESSING_ATTEMPTS - 1));
    private final DatabaseHelper databaseHelper;
    private final RawMsgService rawMsgService;


    @Override
    public <K, V> ConsumerRecords<K, V> handleBatchAndReturnRemaining(Exception thrownException,
                                                                      ConsumerRecords<?, ?> data,
                                                                      Consumer<?, ?> consumer,
                                                                      MessageListenerContainer container,
                                                                      Runnable invokeListener) {

        log.error("При обработке батча сообщений из kafka размером {} из партиций {} произошла ошибка. Все сообщения батча будут сохранены как RawMsg",
                data.count(), data.partitions());
        try {
            for (ConsumerRecord<?, ?> consumerRecord : data)
                rawMsgService.saveAsRawMsg(consumerRecord.value(), thrownException);
        }
        catch (Exception e) {
            log.error("Ошибка при сохранении пропускаемых сообщений kafka как raw message. Батч не будет пропущен.", e);
        }

        return ConsumerRecords.empty();
    }


    @SneakyThrows
    @Override
    public boolean handleOne(Exception exception,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {

        if (databaseHelper.isDatabaseAvailable()) {
            if (recoveryStrategy.recovered(record, exception, container, consumer)) {
                log.error("Было предпринято {} попыток обработки сообщения при наличии подключения к БД." +
                                " Сообщение будет сохранено как RawMsg, а offset будет увеличен! Текущие значения Topic: {} Partition: {} Offset: {}",
                        MSG_PROCESSING_ATTEMPTS, record.topic(), record.partition(), record.offset());
                try {
                    rawMsgService.saveAsRawMsg(record.value(), exception);
                } catch (Exception e) {
                    log.error("Ошибка при сохранении пропускаемого сообщения kafka как raw message. Сообщение не будет пропущено.", e);
                    return false;
                }
                //TODO: проверить, может быть достаточно просто true возвращать. без ручного управления оффсетом
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
                consumer.commitSync();
                return true;
            }
            log.error("Производятся повторные попытки обработки сообщения из кафки при наличии подключения к БД. Offset не будет увеличен." +
                            " Текущие значения Topic: {} Partition: {} Offset: {}",
                    record.topic(), record.partition(), record.offset());
            return false;
        }
        log.error("Во время обработки ошибки при чтении из кафка подключение к БД отсутствует! Offset не будет увеличен." +
                        " Текущие значения Topic: {} Partition: {} Offset: {}",
                record.topic(), record.partition(), record.offset());
        return false;
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