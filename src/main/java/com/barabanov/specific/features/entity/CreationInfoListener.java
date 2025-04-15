package com.barabanov.specific.features.entity;

import jakarta.persistence.PrePersist;

import java.time.OffsetDateTime;


public class CreationInfoListener {

    @PrePersist
    public void setCreationDate(AbstractEntityBase entity) {
        if (entity != null && entity.getCreationDate() == null)
            entity.setCreationDate(OffsetDateTime.now());
    }
}
