package com.example.kafkaProcessorApi;

import com.example.kafkaProcessorApi.MyProcessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;

public class ProcessorApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Topology definition
        Topology topology = new Topology();

        topology.addSource("SourceNode", "input-topic");

        topology.addProcessor("ProcessorNode", MyProcessor::new, "SourceNode");

        topology.addSink("SinkNode", "output-topic", "ProcessorNode");

        System.out.println("--- TOPOLOGY DESCRIPTION ---");
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(exception -> {
            System.out.println("\n💥 UNCAUGHT STREAMS ERROR:");
            exception.printStackTrace();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}
