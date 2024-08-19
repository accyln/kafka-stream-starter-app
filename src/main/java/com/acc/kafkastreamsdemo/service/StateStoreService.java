package com.acc.kafkastreamsdemo.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

//@Service
//@Slf4j
public class StateStoreService {

    StreamsBuilderFactoryBean streamsBuilderFactoryBean;


    public StateStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public void createStore(){
        streamsBuilderFactoryBean.getKafkaStreams().store(StoreQueryParameters.fromNameAndType(
                "sdasd", QueryableStoreTypes.keyValueStore()
        ));
    }
}
