package com.acc.kafkastreamsdemo.chatgpt;

import org.apache.kafka.streams.StreamsBuilder;
import com.acc.kafkastreamsdemo.utils.StateStoreConfig;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class MyTopology {

    public static void createTopology(StreamsBuilder builder) {
        // Add the state store to the builder
        builder.addStateStore(StateStoreConfig.createKeyValueStore("event-store"));

        // Create a stream from the input topic
        var acc = builder.stream("input-topic");

    }
}
