package com.acc.kafkastreamsdemo.utils;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StateStoreConfig {
    public static StoreBuilder<KeyValueStore<String, RequestPlan>> createKeyValueStore(String storeName) {
        final JsonSerializer<RequestPlan> jsonSerializer = new JsonSerializer<>();
        final JsonDeserializer<RequestPlan> jsonDeserializer = new JsonDeserializer<>(RequestPlan.class);

        Serde<String> keySerde = Serdes.String();
        Serde<String> valueSerde = Serdes.String();
        Serde<RequestPlan> invSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                invSerde
        );
    }
}
