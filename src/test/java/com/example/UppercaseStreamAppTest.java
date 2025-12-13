package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.*;
import org.junit.jupiter.api.*;

import com.example.kafkaTesting.UppercaseStreamApp;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UppercaseStreamAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;  // key: String, value: String

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(
                UppercaseStreamApp.buildTopology(),
                props
        );

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );

        outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldConvertValueToUppercase() {
        inputTopic.pipeInput("k1", "hello");

        String result = outputTopic.readValue();
        assertEquals("HELLO", result);
    }
}
