import io.github.cdimascio.dotenv.Dotenv;
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
        Dotenv dotenv = Dotenv.configure().filename("client-ssl.properties").load();

        // create properties object of type java.util.Properties
        Properties properties = new Properties();
        // define the address of the cluster
        properties.put("bootstrap.servers", dotenv.get("service-uri"));
        // provide locations and passwords for truststore, keystore and the private key
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", dotenv.get("ssl.truststore.location"));
        properties.put("ssl.truststore.password", dotenv.get("ssl.truststore.password"));
        properties.put("ssl.keystore.type", dotenv.get("ssl.keystore.type"));
        properties.put("ssl.keystore.location", dotenv.get("ssl.keystore.location"));
        properties.put("ssl.keystore.password", dotenv.get("ssl.keystore.password"));
        properties.put("ssl.key.password", dotenv.get("ssl.key.password"));

        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        properties.put("group.id", "first");
        properties.put("auto.offset.reset", "earliest"); //choose from earliest/latest/none

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);



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
