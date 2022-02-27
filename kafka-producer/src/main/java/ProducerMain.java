import static java.lang.Integer.parseInt;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMain {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Logger logger = Logger.getLogger(ProducerMain.class.getName());
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    String message = "";
    List<String> messages = new ArrayList<>();
    int option = -1;
    Scanner scanner = new Scanner(System.in);
    while (option < 0) {
      System.out.println("1 - Send a single message");
      System.out.println("0 - Exit");
      option = parseInt(scanner.nextLine());
      switch (option) {
        case 1:
          System.out.println("Enter the message to be sent to the topic");
          try {
            message = scanner.nextLine();
            messages.add(message);
            System.out.println("Message to be sent: " + message);
          } catch (RuntimeException e) {
            System.out.println("Exception while reading the message");
          }
          break;
        case 0:
          System.out.println("Exiting");
          break;
      }
    }
    if(option > 0) {
      System.out.println("Initializing the producer");
    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    //create producer
      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    //create recorder
      String key = "id_0";
      ProducerRecord<String, String> recordProducer = new ProducerRecord<>("first_topic", key,messages.get(0));

      logger.info("Key" + key); //log the key

      producer.send(recordProducer, new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception == null) {
            logger.info("Received new metadata. \n"
                            + "Topic: " + metadata.topic()
                            + "\n" + "Partition: " + metadata.partition()
                            + "\n" + "offset: " + metadata.offset()
                            + "\n" + "Timestamp: " + metadata.timestamp());
          } else {
            logger.info("Error " + exception.getMessage());
          }
        }
      });

      producer.close();
      messages.clear();
    }
  }
}
