package com.acc.kafkastreamsdemo.chatgpt;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class EventProcessor implements Processor<String, RequestPlan, String, RequestPlan> {
    private KeyValueStore<String, RequestPlan> stateStore;
    private ProcessorContext<String, RequestPlan> context;

    @Override
    public void init(ProcessorContext<String, RequestPlan> context) {
        this.context = context;
        this.stateStore = context.getStateStore("event-store");

        // Schedule punctuator to run every minute (or adjust interval as needed)
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                checkAndEmitEvents(timestamp);
            }
        });
    }

    @Override
    public void process(Record<String, RequestPlan> record) {
        // Store event with its timestamp in the state store
        stateStore.put(record.key(), record.value());
    }

    private void checkAndEmitEvents(long currentTimestamp) {
        // Iterate over all entries in the state store
        stateStore.all().forEachRemaining(entry -> {

                // Emit the event to the sink topic

                context.forward(new Record<>(entry.key, null, currentTimestamp));
                // Remove the event from the state store after it's processed
                stateStore.delete(entry.key);

        });
    }

    @Override
    public void close() {
        // Clean up resources if necessary
    }
}
