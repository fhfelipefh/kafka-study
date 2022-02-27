import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerModified {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ConsumerModified.class);

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

    //pool for new data
    while (true) {

     ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

     for (ConsumerRecord<String, String> record : records)  {
       System.out.println("Key"+record.key());
       System.out.println("Value"+record.value());
       System.out.println("Partition"+record.partition());
     }
    }

  }
}
