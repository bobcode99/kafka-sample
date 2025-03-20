package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;


@Slf4j
public class KafkaRebalanceExample {

    public static void main(String[] args) {
        // First, produce some messages to the topic
        produceMessages();

        // Then run the consumer that will demonstrate the rebalance issue
        runConsumer();
    }

    /**
     * Produces sample messages to the Kafka topic
     */
    private static void produceMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Send 10 messages, with one "slow" message that will trigger our issue
            for (int i = 1; i <= 10; i++) {
                String message = (i == 5) ? "slow-message-" + i : "message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic",
                        "key-" + i,
                        message);

                // Send message synchronously so we can see the results
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Produced message: " + message +
                        " to partition " + metadata.partition() +
                        " with offset " + metadata.offset());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("Producer closed, all messages sent");
        }
    }

    /**
     * Runs a consumer that will demonstrate the rebalance and duplicate processing issue
     */
    private static void runConsumer() {
        // Set up consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Short max poll interval to trigger rebalance quickly
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); // 10 seconds

        // Manual commit mode
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Track which messages we've seen to demonstrate duplicates
        Set<String> processedMessages = new HashSet<>();

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("test-topic"), new RebalanceListener());

        try {
            // Process flag to show when we've been rebalanced
            boolean hasRebalanced = false;

            while (true) {
                // Poll for records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println("Fetched " + records.count() + " records");

                    // Process records
                    for (ConsumerRecord<String, String> record : records) {
                        String messageId = record.key() + "-" + record.value();

                        // Check if we've seen this message before
                        if (processedMessages.contains(messageId)) {
                            System.out.println("DUPLICATE PROCESSING: " + messageId +
                                    " from partition " + record.partition() +
                                    " with offset " + record.offset());
                        } else {
                            System.out.println("Processing: " + messageId +
                                    " from partition " + record.partition() +
                                    " with offset " + record.offset());
                            processedMessages.add(messageId);
                        }

                        // Simulate long processing that will exceed max.poll.interval.ms
                        // This will cause a rebalance and potentially cause duplicate processing
                        if (record.value().contains("slow") && !hasRebalanced) {
                            System.out.println("Simulating slow processing...");
                            try {
                                Thread.sleep(15000); // 15 seconds, exceeding our 10 second max poll interval
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println("Slow processing complete");
                            hasRebalanced = true;
                        }
                    }

                    // Only commit after processing all records
                    // Uncomment this line to prevent duplicates
                    // consumer.commitSync();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    static class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("REBALANCE EVENT: Partitions revoked: " + partitions);
            // Here you could commit offsets for the partitions that were revoked
            // to avoid duplicate processing, but we're intentionally not doing that
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("REBALANCE EVENT: Partitions assigned: " + partitions);
        }
    }
}