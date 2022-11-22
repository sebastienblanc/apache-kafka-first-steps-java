package org.example;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerOneMessage {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerOneMessage.class.getName());

        // step # 1: create a producer and connect to the cluster
        // get connection data from the configuration file
        
        Properties properties = Utils.loadProperties();
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String> producer =
                new KafkaProducer<>(properties);

        // step # 2: define the topic name
        String topicName = "customer-activity";

        // step # 3: create a message record
        JSONObject message = new JSONObject();
        message.put("customer", "Judy Hopps\uD83D\uDC30");
        message.put("product", "Carrot \uD83E\uDD55");
        message.put("operation", "ordered");
        // package the message in the record
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topicName, message.toString());
        logger.info("Record created: " + record);

        // step # 4: send data to the cluster
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    logger.info("Sent successfully. Metadata: " + metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
