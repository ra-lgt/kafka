package com.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        // Kafka broker address
        String bootstrapServers = "localhost:9092";
        String topic = "demo-topic";

        // Producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 10; i++) {
                String message = "Hello Kafka Message " + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key" + i, message);  // Topic, Key, Value

                producer.send(record, (metadata, exception) -> {

                    if (exception == null) {
                        System.out.println("Sent: " + message + " → Partition: " +metadata.partition() + " Offset: " + metadata.offset());
                    } 
                    
                    else {
                        exception.printStackTrace();
                    }
                });
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
