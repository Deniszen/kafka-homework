import utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Utils.recreateTopics(1, 1, "topic1", "topic2");

        try (var producer = new KafkaProducer<String, String>(Utils.createProducerConfig(b -> {
            b.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ex5");
        }))) {
            producer.initTransactions();

            producer.beginTransaction();
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("topic1", "confirmed-t1-" + i));
                producer.send(new ProducerRecord<>("topic2", "confirmed-t2-" + i));
            }
            producer.commitTransaction();

            producer.beginTransaction();
            for (int i = 0; i < 2; i++) {
                producer.send(new ProducerRecord<>("topic1", "aborted-t1-" + i));
                producer.send(new ProducerRecord<>("topic2", "aborted-t2-" + i));
            }
            producer.abortTransaction();
        }

        Properties consumerProps = new Properties();
        consumerProps.putAll(Utils.consumerConfig);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of("topic1", "topic2"));

            while (true) {
                var records = consumer.poll(Duration.ofMillis(1_000));
                if (records.isEmpty()) break;
                records.forEach(rec ->
                        System.out.printf("topic=%s, key=%s, value=%s, offset=%d%n",
                                rec.topic(), rec.key(), rec.value(), rec.offset())
                );
            }
        }
    }
}
