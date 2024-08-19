package com.acc.kafkastreamsdemo.deneme2;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

import static java.util.Map.entry;
import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
public class KafkaConfig {
    private final static String inputTopic = "example-input-topic";

    private final static String outputTopic = "example-output-topic";

    private final static String bootstrapServers = "localhost:9092";

    private final static String applicationId = "kafka-producer-api-example-application";

    @Bean
    public KafkaStreamsConfiguration kafkaStreamsConfigConfiguration() {
        return new KafkaStreamsConfiguration(
                Map.ofEntries(
                        entry(APPLICATION_ID_CONFIG, applicationId),
                        entry(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()),
                        entry(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                        entry(NUM_STREAM_THREADS_CONFIG, 1),
                        entry(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()),
                        entry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()),
                        entry(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")));
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig(final String bootstrapServers) {
        Map props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "kafka-streams-demo");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        return new KafkaStreamsConfiguration(props);
    }



    @Bean("nopainStreamsBuilderFactoryBean")
    @Primary
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration kafkaStreamsConfigConfiguration)
            throws Exception {

        StreamsBuilderFactoryBean streamsBuilderFactoryBean =
                new StreamsBuilderFactoryBean(kafkaStreamsConfigConfiguration);
        streamsBuilderFactoryBean.afterPropertiesSet();
        streamsBuilderFactoryBean.setInfrastructureCustomizer(new CustomInfrastructureCustomizer(inputTopic, outputTopic));
        streamsBuilderFactoryBean.setCloseTimeout(10); //10 seconds
        return streamsBuilderFactoryBean;
    }

    @Bean
    public NewTopic greetingsTopic(){
        return TopicBuilder.name(inputTopic)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic greetingsOutputTopic(){
        return TopicBuilder.name(outputTopic)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public Serde<RequestPlan> commitSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(RequestPlan.class));
    }
}
