package com.barabanov.specific.features;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;


@Slf4j
@RequiredArgsConstructor
@Service
public class BicycleHandler {

    private final DatabaseHelper databaseHelper;


    public void handleBicycleBatch(List<Bicycle> bicycleList) {

        // эмуляция взаимодействия с БД
        if (!databaseHelper.isDatabaseAvailable())
            throw new RuntimeException("Не удалось сохранить информацию о машине в БД т.к. отсутствует подключение к БД.");

        log.info("Обработаны велосипеды моделей: {}",
                bicycleList.stream()
                        .map(Bicycle::model)
                        .toList());
    }
}
