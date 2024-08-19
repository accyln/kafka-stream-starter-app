package com.acc.kafkastreamsdemo.event;

import com.acc.kafkastreamsdemo.domain.Environment;
import com.acc.kafkastreamsdemo.domain.OrderType;

import java.time.LocalDateTime;

public record RequestPlan(Integer planId,
                          OrderType orderType,
                          Environment environment,
                          LocalDateTime scheduledTime) {
    @Override
    public Integer planId() {
        return planId;
    }

    @Override
    public OrderType orderType() {
        return orderType;
    }

    @Override
    public Environment environment() {
        return environment;
    }

    @Override
    public LocalDateTime scheduledTime() {
        return scheduledTime;
    }
}
