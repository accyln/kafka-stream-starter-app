package com.acc.kafkastreamsdemo.launcher;

import com.acc.kafkastreamsdemo.domain.Environment;
import com.acc.kafkastreamsdemo.domain.OrderType;
import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.acc.kafkastreamsdemo.processor.LoadBalancerProcessor;
import com.acc.kafkastreamsdemo.topology.MessageTopology;
import com.acc.kafkastreamsdemo.topology.RequestPlanTopology;
import com.acc.kafkastreamsdemo.utils.RequestPredicates;
import com.acc.kafkastreamsdemo.utils.StateStoreConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class RequestPlanApp {

    public static void main(String[] args) {

        var requestPlanTopology= RequestPlanTopology.buildTopology();

        StateStoreConfig stateStoreConfig=new StateStoreConfig();



        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "scheduler-app"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        var kafkaStreams= new KafkaStreams(requestPlanTopology,config);
        createTopics(config, List.of(RequestPlanTopology.REQUEST_PLAN_TOPIC,RequestPlanTopology.APPLY_DNS_PLAN_TOPIC,RequestPlanTopology.APPLY_F5_PLAN_TOPIC,"example-input-topic"));


        for (Environment env : Environment.values()) {
            for (OrderType type : OrderType.values()) {

                createTopic(config, env.name().toLowerCase() + "-" + type.name().toLowerCase() + "-stream");
            }
        }

        //final Topology builder = new Topology();


//        requestPlanTopology.addSource("Source", "newtesttopic")
//                .addProcessor("LoadBalancerProcessor", () -> new LoadBalancerProcessor(), "Source")
//                .addStateStore(StateStoreConfig.createKeyValueStore(RequestPlanTopology.SCHEDULER_DEV_STORE), "LoadBalancerProcessor")
//                .addSink("Sink", RequestPlanTopology.APPLY_DNS_PLAN_TOPIC, "LoadBalancerProcessor");

//        requestPlanTopology.addSource("Source", "anan")
//                .addProcessor("LoadBalancerProcessor", () -> new LoadBalancerProcessor(), "Source").
//                connectProcessorAndStateStores("LoadBalancerProcessor",RequestPlanTopology.SCHEDULER_DEV_STORE);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("Starting Greeting streams");
        try {
            kafkaStreams.start();
        }catch (Exception e){

        }




    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
    private static void createTopic(Properties config, String topicc) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopic = new NewTopic(topicc, partitions, replication);
        List<NewTopic> list = List.of(newTopic);


        var createTopicResult = admin.createTopics(list);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }

        
}
