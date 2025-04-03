package com.barabanov.specific.features;

import java.time.Instant;

public record Bicycle(
        String brand,
        String model,
        Instant createDate) {
}
