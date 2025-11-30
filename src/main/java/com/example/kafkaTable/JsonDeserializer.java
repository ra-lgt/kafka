package com.example.kafkaTable;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final Gson gson = new Gson();
    private Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return gson.fromJson(new String(bytes), clazz);
    }
}