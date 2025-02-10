package com.barabanov;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.*;

import java.time.LocalDateTime;

@Data
@Builder
@EqualsAndHashCode(exclude = "objectId")
@AllArgsConstructor
@NoArgsConstructor
@Entity(name = "KafkaListenerError")
public class KafkaListenerErrorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long objectId; //TODO: удалить при внедрении в проект

    private String errorStackTrace;

    private String kafkaMessageJson;

    private String kafkaTopicName;

    private LocalDateTime timeOfLastProcessingAttempt;
}
