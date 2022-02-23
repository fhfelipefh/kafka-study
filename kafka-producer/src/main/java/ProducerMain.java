import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMain {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    String message = "";
    List<String> messages = new ArrayList<>();
    int option = -1;
    System.out.println("Enter the message to be sent to the topic");
    Scanner scanner = new Scanner(System.in);
    while (option != 0) {
      System.out.println("1 - Send a single message");
      System.out.println("2 - Send multiple messages");
      System.out.println("0 - Exit");
      option = scanner.nextInt();
      switch (option) {
        case 1:
          System.out.println("Enter the message to be sent to the topic");
          try {
            message = scanner.nextLine();
            messages.add(message);
          } catch (RuntimeException e) {
            System.out.println("Exception while reading the message");
            message = e.getMessage() + " :)";
          }
          System.out.println("Message to be sent: " + message);
          break;
        case 2:
          System.out.println("Enter the number of messages to be sent to the topic");
          int numberOfMessages = scanner.nextInt();
          for (int i = 0; i <= numberOfMessages; i++) {
            try {
              message = scanner.nextLine();
              messages.add(message);
              System.out.println("Message added, next message: ");
            } catch (RuntimeException e) {
              System.out.println("Exception while reading the message");
              message = e.getMessage() + " :)";
            }
          }
          System.out.println("All messages added");
          break;
        case 0:
          System.out.println("Exiting");
          break;
      }
    }
    if(option != 0) {
    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      //create producer recordProducer
      for (int i = 0; i < messages.size(); i++) {
        ProducerRecord<String, String> recordProducer = new ProducerRecord<>("first_topic", messages.get(i));
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
      messages.clear();
    }
    }
  }
}
