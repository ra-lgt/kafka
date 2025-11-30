package com.example.kafkaTable;


import com.example.kafkaTable.ProductSale;
import com.example.kafkaTable.JsonSerde;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class KTableApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-sales-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<ProductSale> productSaleSerde = new JsonSerde<>(ProductSale.class);

        // STEP 1: Read stream of sales
        KStream<String, ProductSale> salesStream =builder.stream("sales", Consumed.with(Serdes.String(), productSaleSerde)); // Object -> Json (Js)

        // STEP 2: Convert to KTable with aggregation
        KTable<String, Integer> salesAgg = salesStream
            .peek((k,v) -> System.out.println("Incoming: " + v.getProductId() + " -> " + v.getQuantity()))
            .groupBy(
                    (key, value) -> {
                        System.out.println("Grouping by key: " + value.getProductId());
                        return value.getProductId();
                    },
                    Grouped.with(Serdes.String(), productSaleSerde)
            )
            .aggregate(
                    () -> 0,
                    (productId, sale, total) -> {
                        int newTotal = total + sale.getQuantity();
                        System.out.println("AGG: " + productId + " new total = " + newTotal);
                        return newTotal;
                    },
                    Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("sales-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Integer())
            );



        // STEP 3: Output aggregated updates
        salesAgg.toStream().to("sales-agg", Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // STEP 4: Query the state store (read-only)
        Thread.sleep(10000); // wait for store to initialize

        ReadOnlyKeyValueStore<String, Integer> store =
        streams.store(
            StoreQueryParameters.fromNameAndType(
                "sales-store",
                QueryableStoreTypes.<String, Integer>keyValueStore()
            )
        );



        System.out.println("Current product totals:");
        System.out.println("Product A: " + store.get("A"));
        System.out.println("Product B: " + store.get("B"));

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
