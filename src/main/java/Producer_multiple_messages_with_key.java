import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class Producer_multiple_messages_with_key {
    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(Producer_multiple_messages_with_key.class.getName());

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

        // step # 3: generate and send message data
        while(true) {
            // generate a new message
            JSONObject message = generateMessage();

            // create a producer record
            String key = message.get("customer").toString();
            String value = message.toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            logger.info("Record created: " + record);

            // send data
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
            Thread.sleep(1000);
        }
    }

    static final String[] operations = {"searched", "ordered"};
    static final String[] customers = {"Judy Hopps\uD83D\uDC30", "Nick Wilde\uD83E\uDD8A", "Chief Bogo\uD83D\uDC03", "Officer Clawhauser\uD83D\uDE3C", "Mayor Lionheart \uD83E\uDD81", "Mr. Big \uD83E\uDE91", "Fru Fru\uD83D\uDC90"};
    static final String[] products = {"Donut \uD83C\uDF69 ", "Carrot \uD83E\uDD55", "Tie \uD83D\uDC54", "Glasses \uD83D\uDC53️", "Phone ☎️", "Ice cream \uD83C\uDF68", "Dress \uD83D\uDC57", "Pineapple pizza \uD83C\uDF55"};

    public static JSONObject generateMessage() {
        JSONObject message = new JSONObject();

        // randomly assign values
        Random randomizer = new Random();
        message.put("customer", customers[randomizer.nextInt(7)]);
        message.put("product", products[randomizer.nextInt(7)]);
        message.put("operation", operations[randomizer.nextInt(30) < 25 ? 0 : 1]); //prefer 'search' over 'ordered'

        return message;
    }
}
