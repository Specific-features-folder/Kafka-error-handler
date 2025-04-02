package com.barabanov.specific.features;

import java.time.Instant;

public record Car(
        String brand,
        String model,
        Instant createDate) {
}
