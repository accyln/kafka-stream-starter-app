package com.acc.kafkastreamsdemo.deneme2;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.acc.kafkastreamsdemo.topology.RequestPlanTopology;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

public class CustomInfrastructureCustomizer implements KafkaStreamsInfrastructureCustomizer {
    private static final Serde<Long> KEY_SERDE = Serdes.Long();

    private static final Serde<RequestPlan> VALUE_SERDE = new JsonSerde<>(RequestPlan.class).ignoreTypeHeaders();

    private static final String SINGERS_K_TABLE_NAME = "nopain-singers-k-table";

    private static final Deserializer<String> KEY_JSON_DE = new JsonDeserializer<>(String.class);

    private static final Deserializer<RequestPlan> VALUE_JSON_DE =
            new JsonDeserializer<>(RequestPlan.class).ignoreTypeHeaders();

    private final String inputTopic;

    private final String outputTopic;

    CustomInfrastructureCustomizer(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public void configureBuilder(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<Long, RequestPlan>> stateStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(RequestPlanTopology.SCHEDULER_DEV_STORE), KEY_SERDE, VALUE_SERDE);

        Topology topology = builder.build();
        topology.addSource("Source", KEY_JSON_DE, VALUE_JSON_DE, inputTopic)
                .addProcessor("Process", () -> new CustomProcessor(RequestPlanTopology.SCHEDULER_DEV_STORE), "Source")
                .addStateStore(stateStoreBuilder, "Process")
                .addSink("Sink", outputTopic, "Process");


    }



}
