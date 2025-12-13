package com.example.kafkaProcessorApi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MyProcessor implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        System.out.println("Processor initialized.");
    }

    @Override
    public void process(String key, String value) {
        System.out.println("PROCESSING: key=" + key + ", value=" + value);
        if (value == null) {
            System.out.println("❌ NULL VALUE DETECTED!");
        }
        context.forward(key, value == null ? "NULL" : value.toUpperCase());  // ajay -> AJAY
    }

    @Override
    public void close() {
        System.out.println("Processor closed.");
    }
}