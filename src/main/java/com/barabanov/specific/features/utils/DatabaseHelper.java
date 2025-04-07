package com.barabanov.specific.features.utils;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;


@RequiredArgsConstructor
@Component
public class DatabaseHelper {

    private final DataSource dataSource;


    public boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }
}
