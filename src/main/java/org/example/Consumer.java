package org.example;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        // step # 1: create a consumer and connect to the cluster
        // get connection data from the configuration file
        Properties properties = Utils.loadProperties();
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", "first");
        properties.put("auto.offset.reset", "earliest"); //choose from earliest/latest/none

        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties)) {
            // step # 2 subscribe consumer to the topic
            String topicName = "customer-activity";
            consumer.subscribe(Collections.singleton(topicName));

            // step # 3 poll andprocess new data
            while (true) {
                // poll new data
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                // process new data
                for (ConsumerRecord<String,String> record : records) {
                    logger.info("partition " + record.partition() +
                            "| offset " + record.offset() +
                            "| " + record.value() );
                }
            }
        }

    }
}
