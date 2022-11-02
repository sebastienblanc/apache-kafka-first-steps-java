import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer_one_message {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Producer_one_message.class.getName());

        // step # 1: create a producer and connect to the cluster
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

        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String> producer =
                new KafkaProducer<>(properties);

        // step # 2: define the topic name
        String topicName = "customer-activity";

        // step # 3: create a message record
        JSONObject message = new JSONObject();
        message.put("customer", "Marty");
        message.put("product", "skateboard");
        message.put("operation", "ordered");
        // package the message in the record
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topicName, message.toString());

        // step # 4: send data to the cluster
        producer.send(record);
        logger.info("Sent: " + message);
    }
}
