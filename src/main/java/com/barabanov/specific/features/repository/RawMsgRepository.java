package com.barabanov.specific.features.repository;

import com.barabanov.specific.features.entity.RawMsgEntity;
import org.springframework.data.repository.CrudRepository;


public interface RawMsgRepository extends CrudRepository<RawMsgEntity, Long> {
}
