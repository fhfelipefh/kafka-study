import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerMainWithShutDown {

  public static void main(String[] args) {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app-3");

    // get the reference to the current thread
    final Thread currentThread = Thread.currentThread();



    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(java.util.Arrays.asList("first_topic"));
      System.out.println("O consumidor está aguardando mensagens...");

      // adding a shutdown hook to the current thread
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          System.out.println("Shutdown hook called");
          currentThread.interrupt();
        }
      });

      while (true) {
        ConsumerRecords<String, String> cont = consumer.poll(Duration.ofMillis(100));
        if (cont.count() > 0) {
          for (ConsumerRecord<String, String> entry : cont) {
            System.out.println(entry.value());
          }
        }

      }
    }
  }
}
