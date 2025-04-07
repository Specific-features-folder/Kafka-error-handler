package com.barabanov.specific.features.service;

import com.barabanov.specific.features.entity.RawMsgEntity;
import com.barabanov.specific.features.repository.RawMsgRepository;
import com.barabanov.specific.features.utils.JsonHelper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Slf4j
@RequiredArgsConstructor
@Service
public class RawMsgService {

    private final JsonHelper jsonHelper;
    private final RawMsgRepository rawMsgRepository;


    @Transactional
    public void saveAsRawMsg(Object msg, Exception exception) {
        rawMsgRepository.save(RawMsgEntity.builder()
                .creationDate(OffsetDateTime.now())
                .msgJson(jsonHelper.toJson(msg))
                .errorText(exception.getMessage())
                .build());
    }
}
