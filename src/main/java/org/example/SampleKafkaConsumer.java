package org.example;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SampleKafkaConsumer.class);

    public static void main(String[] args) {
        String topic = KafkaConst.TOPIC;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConst.GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // Disable auto commit to simulate duplicate message processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Set a low max poll interval to force a rebalance when delay happens
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000"); // 10 seconds

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumed: key={}, value={}, partition={}, offset={}",
                            record.key(), record.value(), record.partition(), record.offset());

                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Consumer error: {}", e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
