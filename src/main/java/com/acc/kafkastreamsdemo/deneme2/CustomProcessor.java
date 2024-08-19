package com.acc.kafkastreamsdemo.deneme2;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Objects;

import static com.acc.kafkastreamsdemo.topology.RequestPlanTopology.SCHEDULER_DEV_STORE;

@Slf4j
public class CustomProcessor implements Processor<String, RequestPlan,String, RequestPlan> {
    private static final Integer SCHEDULE = 60; // second, for test

    private KeyValueStore<String, RequestPlan> stateStore;
    private ProcessorContext<String, RequestPlan> context;


    private final String stateStoreName;

    public CustomProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
        this.context = context;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        stateStore = (KeyValueStore<String, RequestPlan>) processorContext.getStateStore(stateStoreName);

        // Scheduler runs Punctuator every 60 seconds.
        processorContext.schedule(Duration.ofSeconds(SCHEDULE), PunctuationType.WALL_CLOCK_TIME,this::forwardTombstones);
        Objects.requireNonNull(stateStore, "State store can't be null");
    }

    @Override
    public void process(Record<String, RequestPlan> record) {
        System.out.println("Key: " + record.key() + " Value: " + record.value());

        stateStore.put(record.key(), record.value());
    }

    @Override
    public void close() {

    }

    private void forwardTombstones(long timestamp) {
        log.info("Resetting {} store ", SCHEDULER_DEV_STORE);

        try (KeyValueIterator<String, RequestPlan> iterator = stateStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, RequestPlan> keyValue = iterator.next();
                context.forward(new Record<>(keyValue.key, null, timestamp));
            }
        }
    }

}
