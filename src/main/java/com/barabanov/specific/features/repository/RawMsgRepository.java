package com.barabanov.specific.features.repository;

import com.barabanov.specific.features.entity.RawMsgEntity;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.hibernate.LockOptions;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.CrudRepository;

import java.time.OffsetDateTime;
import java.util.List;


public interface RawMsgRepository extends CrudRepository<RawMsgEntity, Long> {


//    @Lock(LockModeType.PESSIMISTIC_WRITE)
//    @QueryHints(
//            @QueryHint(name = AvailableSettings.JAKARTA_LOCK_TIMEOUT, value = "" + LockOptions.SKIP_LOCKED)
//    )
    @Query(value = """
            SELECT *
            FROM {h-schema}raw_message r
            WHERE r.last_processing_date < :processDateThreshold
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<RawMsgEntity> findRawMsgBatch(Integer batchSize, OffsetDateTime processDateThreshold);
}
