package com.example.kafkaProcessorApi.kafkaPunctuators;

import org.apache.kafka.streams.processor.*;
import java.time.Duration;

public class HeartbeatProcessor implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        // Schedule punctuator
        context.schedule(
                Duration.ofSeconds(5),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    context.forward("heartbeat", "ALIVE @ " + timestamp);
                }
        );
    }

    @Override
    public void process(String key, String value) {
        // normal processing (optional)
        context.forward(key, value);
    }

    @Override
    public void close() {}
}
