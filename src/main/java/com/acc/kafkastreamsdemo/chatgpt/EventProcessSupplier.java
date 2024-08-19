package com.acc.kafkastreamsdemo.chatgpt;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class EventProcessSupplier implements ProcessorSupplier<String, RequestPlan, String, RequestPlan> {

    @Override
    public EventProcessor get() {
        return new EventProcessor();
    }
}
