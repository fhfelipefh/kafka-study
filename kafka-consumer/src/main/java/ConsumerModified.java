import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerModified {

  public static void main(String[] args) {


    String bootstrapServers = "localhost:9092";
    String groupId = "group1";
    String topic = "first_topic";

    //create consumer configs
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    //create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    //subscribe consumer to our topics
    consumer.subscribe(Collections.singleton(topic));

    // assign
    TopicPartition partition = new TopicPartition(topic, 0);
    long offsetToReadFrom = 15L;

    int numberOfMessagesToRead = 5;
    boolean keepOnReading = true;
    int numberOfMessagesReadSoFar = 0;

    System.out.println("Consumer is now reading messages from offset " + offsetToReadFrom);
    //pool for new data
    while (keepOnReading) {

     ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

     for (ConsumerRecord<String, String> record : records)  {
       System.out.println("Key"+record.key());
       System.out.println("Value"+record.value());
       System.out.println("Partition"+record.partition());
       System.out.println("Offset"+record.offset());
       System.out.println("Timestamp"+record.timestamp());
       System.out.println("Topic"+record.topic());
       System.out.println("==========================");
       numberOfMessagesReadSoFar++;
       if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
         keepOnReading = false;
         break;
       }

       System.out.println("========================== exit");

     }
    }

  }
}
