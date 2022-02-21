package com.fhfelipefh.kafka;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerMain {

  public static void main(String[] args) {
    final StringBuilder msg = new StringBuilder("");
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app");
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(java.util.Arrays.asList("first_topic"));
      System.out.println("O consumidor está aguardando mensagens...");
      while (true) {
        ConsumerRecords<String, String> cont = consumer.poll(Duration.ofMillis(500));
        if (cont.count() > 0) {
          for (ConsumerRecord<String, String> entry : cont) {
            System.out.println(entry.value());
            msg.append(entry.value());
          }
        }
        if(cont.count() == 10){
          System.out.println(msg.toString());
          break;
        }
      }
    }
  }
}
