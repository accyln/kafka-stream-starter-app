package com.acc.kafkastreamsdemo.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

public class F5Processor implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new LoadBalancerProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("Counts"),
                Serdes.String(),
                Serdes.Integer()));
    }
}
