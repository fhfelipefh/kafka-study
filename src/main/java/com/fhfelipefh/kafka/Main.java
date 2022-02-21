package com.fhfelipefh.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";

    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                           StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                           StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    //create producer record
    ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello word!");
    //send data
    try {
      producer.send(record).get();
      System.out.println("send data");
    } catch (Exception e) {
      System.out.println("Msg not sent");
    }
  }
}
