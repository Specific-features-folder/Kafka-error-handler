package com.barabanov.specific.features.handler;

import com.barabanov.specific.features.service.RawMsgService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Slf4j
@RequiredArgsConstructor
@Service
public class RawMsgHandler {

    private final RawMsgService rawMsgService;


    public int reworkRawMsgs() {
        // + тут различный фильтры, маппинги и др. логика

        log.info("Начало повторной обработки сообщений сохранённых как raw msg");
        int reworkMsgCount = rawMsgService.reworkRawMsgs();
        log.info("Повторная обработка raw message сообщений была завершена. Было переработано: {} сообщений", reworkMsgCount);

        return reworkMsgCount;
    }
}
