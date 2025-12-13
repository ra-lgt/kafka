package com.example.kafkaProcessorApi.kafkaPunctuators;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import java.util.Properties;

public class ProcessorPunctuatorApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-punctuator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();

        topology.addSource("Source", "input-topic");
        topology.addProcessor("Heartbeat", HeartbeatProcessor::new, "Source");
        topology.addSink("Sink", "output-topic", "Heartbeat");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
