package org.example.eos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//seems won't work
public class ExactlyOnceSimpleExample {
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceSimpleExample.class);
    private static final String INPUT_TOPIC = "simple-topic";
    private static final String OUTPUT_TOPIC = "simple-output";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-eos-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "0");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000"); // 5 seconds to force rebalance
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");  // 10 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000"); // 1 second heartbeat

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> processedStream = inputStream.mapValues(value -> {
            logger.info("Processing value: {}", value);
            try {
                logger.info("Starting sleep for value={}", value);
                Thread.sleep(10000); // 10-second sleep to exceed max.poll.interval.ms
                logger.info("Finished sleep for value={}", value);
            } catch (InterruptedException e) {
                logger.error("Sleep interrupted for value={}", value, e);
                Thread.currentThread().interrupt();
            }
            return value.toUpperCase();
        });

        processedStream.to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        logger.info("Kafka Streams application with EOS started.");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}