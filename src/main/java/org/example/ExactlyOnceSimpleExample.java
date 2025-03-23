package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ExactlyOnceSimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceSimpleExample.class);
    private static final String TOPIC = KafkaConst.TOPIC;

    public static void main(String[] args) {
        // Configure Kafka Streams with EOS
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-eos-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); // Enable EOS
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "0"); // Process immediately
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000"); // Explicitly set to 5 seconds

        // Build the topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(TOPIC);

        // Process messages with a 10-second sleep to simulate long processing
        inputStream.foreach((key, value) -> {
            logger.info("Processing: key={}, value={}", key, value);
            try {
                logger.info("Starting sleep for key={}", key);
                Thread.sleep(10000); // 10-second sleep, exceeds 5-second max.poll.interval.ms
                logger.info("Finished sleep for key={}", key);
            } catch (InterruptedException e) {
                logger.error("Sleep interrupted for key={}", key, e);
                Thread.currentThread().interrupt();
            }
            // No explicit commit needed; EOS handles it transactionally
        });

        // Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        logger.info("Kafka Streams application with EOS started.");

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}