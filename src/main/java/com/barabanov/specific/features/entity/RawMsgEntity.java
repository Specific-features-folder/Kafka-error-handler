package com.barabanov.specific.features.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;


@Entity
@Table(name = "raw_message")
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
@Data
@SuperBuilder
public class RawMsgEntity extends AbstractEntityBase {

    private String topicName;

    private String errorText;

    @JdbcTypeCode(SqlTypes.JSON)
    private String msgJson;

    private OffsetDateTime lastProcessingDate;
}
