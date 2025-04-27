package com.barabanov.specific.features.controller;

import com.barabanov.specific.features.handler.RawMsgHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RequiredArgsConstructor
@RequestMapping(path = "/raw-message")
@RestController
public class RawMsgController {

    private final RawMsgHandler rawMsgHandler;


    @PostMapping("/rework-raw-msg")
    public Integer reworkRawMsg() {
        log.info("Получен REST запрос на переработку сообщений сохранённых как raw message");
        return rawMsgHandler.reworkRawMsgs();
    }
}
