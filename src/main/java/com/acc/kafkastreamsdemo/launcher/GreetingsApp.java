package com.acc.kafkastreamsdemo.launcher;

import com.acc.kafkastreamsdemo.producer.RequestPlanProducer;
import com.acc.kafkastreamsdemo.producer.RequestPlanService;
import com.acc.kafkastreamsdemo.topology.MessageTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        var greetingsTopology= MessageTopology.buildTopology();

        var kafkaStreams= new KafkaStreams(greetingsTopology,config);
        createTopics(config, List.of(MessageTopology.GREETINGS_TOPIC,MessageTopology.GREETINGS_OUTPUT_TOPIC));
        //var kafkaStreams = new KafkaStreams(kTableTopology, config);

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
}
