package com.barabanov;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final DataSource dataSource;


    @KafkaListener(topics = "my-topic",
            properties = "spring.json.value.default.type=com.barabanov.Lada",
            batch = "true")
    public void listenMsg(List<Lada> listOfLadas) {
//        if (!isDatabaseAvailable())
//            throw new RuntimeException("Ошибка из-за отключенной БД");

        System.out.println("Считан батч размером: " + listOfLadas.size());
        System.out.println("Считаны лады моделей: " + listOfLadas.stream()
                .map(Lada::model)
                .toList());

        listOfLadas.stream()
                .filter(lada -> "business-error".equals(lada.model()))
                .findAny()
                .ifPresent(ladaError -> {
                    throw new RuntimeException("Бизнесовая ошибка");
                });
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }
}
