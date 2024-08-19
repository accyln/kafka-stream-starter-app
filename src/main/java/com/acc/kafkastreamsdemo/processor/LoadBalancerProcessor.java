package com.acc.kafkastreamsdemo.processor;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Locale;

import static com.acc.kafkastreamsdemo.topology.RequestPlanTopology.SCHEDULER_DEV_STORE;

@Slf4j
public class LoadBalancerProcessor extends ContextualProcessor<String, RequestPlan, String, RequestPlan> {
    private KeyValueStore<String, RequestPlan> personStore;
    private ProcessorContext<String, RequestPlan> context;

    @Override
    public void init(ProcessorContext<String, RequestPlan> context) {
        this.context=context;
        this.personStore = context.getStateStore("scheduler-dev-store");
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::forwardTombstones);
    }

    @Override
    public void process(Record<String, RequestPlan> message) {
        if (message.value() == null) {
            log.info("Received tombstone for key = {}", message.key());
            personStore.delete(message.key());
            return;
        }

        log.info("Received key = {}, value = {}", message.key(), message.value());
        personStore.put(message.key(), message.value());
    }


    private void forwardTombstones(long timestamp) {

        try (KeyValueIterator<String, RequestPlan> iterator = personStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, RequestPlan> keyValue = iterator.next();
                context.forward(new Record<>(keyValue.key, null, timestamp));
            }
        }
    }
}
