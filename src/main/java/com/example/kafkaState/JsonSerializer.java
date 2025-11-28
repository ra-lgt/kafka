package com.example.kafkaState;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;
import java.nio.charset.StandardCharsets;

public class JsonSerializer<T> implements Serializer<T> {
    private final Gson gson = new Gson();
    @Override
    public byte[] serialize(String topic, T data) {
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
}