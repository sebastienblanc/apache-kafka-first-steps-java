package org.example;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class ProducerMultipleMessages {

    static final String[] operations = {"searched", "ordered"};
    static final String[] customers = {"Judy Hopps\uD83D\uDC30", "Nick Wilde\uD83E\uDD8A", "Chief Bogo\uD83D\uDC03", "Officer Clawhauser\uD83D\uDE3C", "Mayor Lionheart \uD83E\uDD81", "Mr. Big \uD83E\uDE91", "Fru Fru\uD83D\uDC90"};
    static final String[] products = {"Donut \uD83C\uDF69 ", "Carrot \uD83E\uDD55", "Tie \uD83D\uDC54", "Glasses \uD83D\uDC53️", "Phone ☎️", "Ice cream \uD83C\uDF68", "Dress \uD83D\uDC57", "Pineapple pizza \uD83C\uDF55"};

    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerMultipleMessages.class.getName());

        // step # 1: create a producer and connect to the cluster
        // get connection data from the configuration file
        Properties properties = Utils.loadProperties();

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(properties)) {
            // step # 2: define the topic name
            String topicName = "customer-activity";

            // step # 3: generate and send message data
            while(true) {
                // generate a new message
                JSONObject message = generateMessage();

                // package the message in a record
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topicName, message.toString());
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
    }

    public static JSONObject generateMessage() {
        JSONObject message = new JSONObject();

        // randomly assign values
        Random randomizer = new Random();
        message.put("customer", customers[randomizer.nextInt(7)]);
        message.put("product", products[randomizer.nextInt(7)]);
        message.put("operation", operations[randomizer.nextInt(30) < 25 ? 0 : 1]); // prefer 'search' over 'ordered'

        return message;
    }
}
