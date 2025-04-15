package com.barabanov.specific.features.service;

import com.barabanov.specific.features.Bicycle;
import com.barabanov.specific.features.Car;
import com.barabanov.specific.features.entity.RawMsgEntity;
import com.barabanov.specific.features.handler.BicycleHandler;
import com.barabanov.specific.features.handler.CarHandler;
import com.barabanov.specific.features.repository.RawMsgRepository;
import com.barabanov.specific.features.utils.JsonHelper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class RawMsgService {

    @Value("${performance.raw-msg.rework-msg-batch-size:10}")
    private final Integer reworkMsgBatchSize;
    @Value("${kafka-topics.car-topic}")
    private final String carTopicName;
    @Value("${kafka-topics.bicycle-topic}")
    private final String bicycleTopicName;

    private final JsonHelper jsonHelper;
    private final RawMsgRepository rawMsgRepository;
    private final TransactionTemplate transactionTemplate;
    private final BicycleHandler bicycleHandler;
    private final CarHandler carHandler;


    @Transactional
    public void saveAsRawMsg(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        OffsetDateTime nowDate = OffsetDateTime.now();

        rawMsgRepository.save(RawMsgEntity.builder()
                .msgJson(jsonHelper.toJson(consumerRecord.value()))
                .topicName(consumerRecord.topic())
                .errorText(Optional.ofNullable(exception.getStackTrace())
                        .map(Arrays::toString)
                        .orElse(exception.getMessage()))
                .lastProcessingDate(nowDate)
                .creationDate(nowDate)
                .build());
    }


    public int reworkRawMsgs() {
        OffsetDateTime reworkStartProcessDate = OffsetDateTime.now();
        int reworkedMsgsCount = 0;

        while (true) {
            Integer reworkedMsgsInBatch = transactionTemplate.execute(transactionStatus -> {
                List<RawMsgEntity> rawMsgBatch = rawMsgRepository.findRawMsgBatch(reworkMsgBatchSize, reworkStartProcessDate);
                if (rawMsgBatch.isEmpty())
                    return null;

                return handleRawMsgBatch(rawMsgBatch);
            });
            if (reworkedMsgsInBatch == null)
                break;

            reworkedMsgsCount += reworkedMsgsInBatch;
        }

        return reworkedMsgsCount;
    }


    private int handleRawMsgBatch(List<RawMsgEntity> rawMsgBatch) {
        int reworkedMsgsInBatch = 0;
        for (RawMsgEntity rawMsgEntity : rawMsgBatch) {
            try {
                handleRawMsg(rawMsgEntity);
                reworkedMsgsInBatch++;
                rawMsgRepository.delete(rawMsgEntity);
            } catch (Exception e) {
                log.error("Не удалось обработать raw message с id {} из-за ошибки: {}", rawMsgEntity.getId(), e.getMessage());
                rawMsgEntity.setErrorText(Optional.ofNullable(e.getStackTrace())
                        .map(Arrays::toString)
                        .orElse(e.getMessage()));
                rawMsgEntity.setLastProcessingDate(OffsetDateTime.now());
                rawMsgRepository.save(rawMsgEntity);
            }
        }
        return reworkedMsgsInBatch;
    }


    private void handleRawMsg(RawMsgEntity rawMsgEntity) {

        String msgTopicName = rawMsgEntity.getTopicName();

        if (carTopicName.equals(msgTopicName)) {
            Car car = jsonHelper.fromJson(rawMsgEntity.getMsgJson(), Car.class);
            carHandler.handlerCar(car);
        } else if (bicycleTopicName.equals(msgTopicName)) {
            Bicycle bicycle = jsonHelper.fromJson(rawMsgEntity.getMsgJson(), Bicycle.class);
            bicycleHandler.handleBicycleBatch(List.of(bicycle));
        } else
            throw new RuntimeException(String.format("При обработке raw message не удалось определить обработчик по топику: %s", msgTopicName));
    }

}
