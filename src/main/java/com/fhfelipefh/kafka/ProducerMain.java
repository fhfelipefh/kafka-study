package com.fhfelipefh.kafka;

import static com.fhfelipefh.kafka.RandomData.getRandomName;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMain {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      //create producer recordProducer
      for (int i = 0; i < 10; i++) {
        ProducerRecord<String, String> recordProducer = new ProducerRecord<>("first_topic",
                                                                             getRandomName());
        //send data
        try {
          producer.send(recordProducer, (metadata, exception) -> {
            if (exception != null) {
              exception.printStackTrace();
              System.out.println("Error sending data: " + metadata);
            }
            System.out.println("Data sent successfully: " + metadata.topic());
          }).get();
          System.out.println("send data");
        } catch (Exception e) {
          System.out.println("Msg not sent");
        }
      }
    }
  }
}
