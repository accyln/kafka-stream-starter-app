package com.acc.kafkastreamsdemo.topology;

import com.acc.kafkastreamsdemo.domain.Environment;
import com.acc.kafkastreamsdemo.domain.OrderType;
import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.acc.kafkastreamsdemo.processor.LoadBalancerProcessor;
import com.acc.kafkastreamsdemo.utils.RequestPredicates;
import com.acc.kafkastreamsdemo.utils.StateStoreConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;


import java.util.EnumMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore;

public class RequestPlanTopology {
    public static String REQUEST_PLAN_TOPIC="request-plan-topic";
    public static String APPLY_DNS_PLAN_TOPIC ="apply-dns-plan-topic";
    public static String APPLY_F5_PLAN_TOPIC="apply-F5-plan-topic";

    public static String SCHEDULER_DEV_STORE="scheduler-dev-store";
    public static String SCHEDULER_PROD_STORE="scheduler-prod-store";

    public static Topology buildTopology(){
        StreamsBuilder streamBuilder=new StreamsBuilder();



        Predicate<String,RequestPlan> dnsPredicate= (key,plan)-> plan.orderType().equals(OrderType.DNS);
        Predicate<String,RequestPlan> f5Predicate= (key,plan)-> plan.orderType().equals(OrderType.LOADBALANCER);

        final JsonSerializer<RequestPlan> jsonSerializer = new JsonSerializer<>();
        final JsonDeserializer<RequestPlan> jsonDeserializer = new JsonDeserializer<>(RequestPlan.class);

        Serde<String> keySerde = Serdes.String();
        Serde<String> valueSerde = Serdes.String();
        Serde<RequestPlan> invSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        final StoreBuilder<KeyValueStore<String, RequestPlan>> storeBuilderdns = Stores
                .keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(SCHEDULER_DEV_STORE),
                        Serdes.String(), invSerde
                );
//
//        final StoreBuilder<KeyValueStore<String, RequestPlan>> storeBuilderf5 = Stores
//                .keyValueStoreBuilder(
//                        Stores.persistentKeyValueStore(SCHEDULER_PROD_STORE),
//                        Serdes.String(), invSerde
//                );
//
//        var requestPlanStream =streamBuilder.addStateStore(storeBuilderdns)
//        .addStateStore(storeBuilderf5)
//                .stream(REQUEST_PLAN_TOPIC, Consumed.with(Serdes.String(),Serdes.serdeFrom(new JsonSerializer<RequestPlan>(),
//                new JsonDeserializer<RequestPlan>(RequestPlan.class))));

        var requestPlanStream =streamBuilder
                .addStateStore(storeBuilderdns)
//                .addStateStore(StateStoreConfig.createKeyValueStore(SCHEDULER_DEV_STORE))
//                .addStateStore(StateStoreConfig.createKeyValueStore(SCHEDULER_PROD_STORE))
                .stream("example-input-topic", Consumed.with(Serdes.String(),invSerde));

        Map<Environment, Map<OrderType, KStream<String, RequestPlan>>> branches = new EnumMap<>(Environment.class);

        for (Environment env : Environment.values()) {
            Map<OrderType, KStream<String, RequestPlan>> typeBranches = new EnumMap<>(OrderType.class);
            for (OrderType type : OrderType.values()) {
                Predicate<String, RequestPlan> predicate = RequestPredicates.environmentAndTypeMatches(env, type);
                typeBranches.put(type, requestPlanStream.filter(predicate));
            }

            branches.put(env, typeBranches);
        }

        branches.forEach((env, typeBranchMap) ->
                typeBranchMap.forEach((type, stream) -> {
                        stream.print(Printed.<String, RequestPlan>toSysOut().withLabel(env.name().toLowerCase() + "-" + type.name().toLowerCase() + "-stream"));
                        stream.process(LoadBalancerProcessor::new,
                            "scheduler-dev-store")
                                .to(env.name().toLowerCase() + "-" + type.name().toLowerCase() + "-topic");
                        }
                )
        );


        //requestPlanStream.process(LoadBalancerProcessor::new,"scheduler-dev-store");


//        requestPlanStream
//                .split(Named.as("request-plan-stream"))
//                .branch(dnsPredicate,
//                        Branched.withConsumer(generalOrdersStream -> {
//
//                            generalOrdersStream
//                                    .print(Printed.<String, RequestPlan>toSysOut().withLabel("dnsstream"));
//
//
//                            generalOrdersStream
//                                    .process(LoadBalancerProcessor::new,
//                                            "scheduler-dev-store")
//                                    .to(APPLY_DNS_PLAN_TOPIC);
//
//
////                            generalOrdersStream
////                                    .to(APPLY_DNS_PLAN_TOPIC,
////                                            Produced.with(Serdes.String(), Serdes.serdeFrom(jsonSerializer,jsonDeserializer)));
//
//
//
//                        })
//                )
//                .branch(f5Predicate,
//                        Branched.withConsumer(restaurantOrdersStream -> {
//                            restaurantOrdersStream
//                                    .print(Printed.<String, RequestPlan>toSysOut().withLabel("f5stream"));
//
//                            restaurantOrdersStream
////                                   .process(LoadBalancerProcessor::new, SCHEDULER_PROD_STORE)
//                                    .to(APPLY_F5_PLAN_TOPIC);
//
////                            restaurantOrdersStream
////                                    .to(APPLY_F5_PLAN_TOPIC,
////                                            Produced.with(Serdes.String(), Serdes.serdeFrom(jsonSerializer,jsonDeserializer)));
////
//                        })
//                );

        return streamBuilder.build();
    }
}
