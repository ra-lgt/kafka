package com.example.kafkaState;


import com.example.kafkaState.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.Properties;

public class KafkaStreamsApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rewards-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<Purchase> purchaseSerde = new JsonSerde<>(Purchase.class);
        JsonSerde<RewardAccumulator> rewardSerde = new JsonSerde<>(RewardAccumulator.class);

        // Create a persistent state store
        StoreBuilder<KeyValueStore<String, Double>> rewardStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("rewardStore"),
                        Serdes.String(),
                        Serdes.Double()
                );

        builder.addStateStore(rewardStore);

        // Read purchase stream
        KStream<String, Purchase> purchases =builder.stream("purchases", Consumed.with(Serdes.String(), purchaseSerde));

        // Apply transformer with state store
        KStream<String, RewardAccumulator> rewards =purchases.transformValues(RewardTransformer::new, "rewardStore");

        // Write output to topic
        rewards.to("rewards-output", Produced.with(Serdes.String(), rewardSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
