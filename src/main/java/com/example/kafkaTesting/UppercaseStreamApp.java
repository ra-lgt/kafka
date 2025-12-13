package com.example.kafkaTesting;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class UppercaseStreamApp {

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
               .mapValues(value -> value.toLowerCase())
               .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));  //ajay   -> ajay

        return builder.build();   //stack > stack  -> topology
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "uppercase-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaStreams streams = new KafkaStreams(buildTopology(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

