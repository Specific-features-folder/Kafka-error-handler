package com.barabanov.specific.features.handler;

import com.barabanov.specific.features.Car;
import com.barabanov.specific.features.utils.DatabaseHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;


@Slf4j
@RequiredArgsConstructor
@Service
public class CarHandler {

    private final DatabaseHelper databaseHelper;


    /**
     * Тест кейсы
     *
     * 1. Нормально обработать сообщение без ошибок
     * 2. 3ды попытаться обработать сообщение с "business-error" при наличии подключения к БД, после чего пропустить его
     * 3. Выбросить ошибку из-за недоступности БД и не двигать оффсет пока не заработает подключение к БД, после чего успешно обработать сообщение
     * 4. Выбросить ошибку из-за недоступности БД и не двигать оффсет пока не заработает подключение к БД, после чего 3ды пытаться обработать сообщение с "business-error" и пропустить его
     */
    public void handlerCar(Car car) {
        // эмуляция взаимодействия с БД
        if (!databaseHelper.isDatabaseAvailable())
            throw new RuntimeException("Не удалось сохранить информацию о машине в БД т.к. отсутствует подключение к БД.");

        // эмуляция ошибок, например, из-за некорректных данных
        if (Optional.ofNullable(car.brand())
                .filter(carBrand -> carBrand.equals("business-error"))
                .isPresent()) {
            String errorMsg = String.format("При обработке машины %s случилась бизнесовая ошибка", car);
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        } else
            log.info("Обработана машина: {}", car);
    }

}
