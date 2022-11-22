package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    Logger logger = LoggerFactory.getLogger(Utils.class.getName());

    public static Properties loadProperties()  {
        Properties properties = new Properties();
        try (InputStream input = ProducerOneMessage.class.getClassLoader().getResourceAsStream("client-ssl.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
            }
            properties.load(input);
            properties.put("key.serializer", StringSerializer.class.getName());
            properties.put("value.serializer", StringSerializer.class.getName());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }
    
}
