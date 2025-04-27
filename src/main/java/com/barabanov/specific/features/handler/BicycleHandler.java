package com.barabanov.specific.features.handler;

import com.barabanov.specific.features.Bicycle;
import com.barabanov.specific.features.utils.DatabaseHelper;
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
        // В этом методе должен быть try / catch. В try обработка батча сообщений, в catch цикл, где для каждого элемента ещё один try / catch,
        // где в try обработка одного сообщения, в catch - сохранение сообщения как raw message

        // эмуляция взаимодействия с БД
        if (!databaseHelper.isDatabaseAvailable())
            throw new RuntimeException("Не удалось сохранить информацию о машине в БД т.к. отсутствует подключение к БД.");

        log.info("Обработаны велосипеды моделей: {}",
                bicycleList.stream()
                        .map(Bicycle::model)
                        .toList());
    }
}
