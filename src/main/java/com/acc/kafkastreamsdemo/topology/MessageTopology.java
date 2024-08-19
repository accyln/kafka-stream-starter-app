package com.acc.kafkastreamsdemo.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.http.HttpHeaders;

public class MessageTopology {
    public static String GREETINGS_TOPIC="greetings-topic";
    public static String GREETINGS_OUTPUT_TOPIC="greetings-output-topic";

    public static Topology buildTopology(){
        StreamsBuilder streamBuilder=new StreamsBuilder();

        var greetingsStream=streamBuilder.stream(GREETINGS_TOPIC, Consumed.with(Serdes.String(),Serdes.String()));

        greetingsStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream"));

        var modifiedStream=greetingsStream.mapValues((readOnlyKey, value) -> value.toUpperCase());

        modifiedStream.to(GREETINGS_OUTPUT_TOPIC, Produced.with(Serdes.String(),Serdes.String()));
        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("modifiedStream"));

        return streamBuilder.build();
    }
}
