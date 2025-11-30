package com.example.kafkaTable;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    public JsonSerde(Class<T> clazz) {
        super(new JsonSerializer<>(), new JsonDeserializer<>(clazz));
    }
}
