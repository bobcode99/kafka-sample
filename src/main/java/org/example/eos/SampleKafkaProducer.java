package org.example.eos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.KafkaConst;
import org.example.SampleKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SampleKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(SampleKafkaConsumer.class);

    public static void main(String[] args) {
        String topic = KafkaConst.SIMPLE_TOPIC_FOR_EOS;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 21; i <= 30; i++) {
            String key = Integer.toString(i);
            String value = "Message " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Produced: key={}, value={}, partition={}, offset={}",
                            key, value, metadata.partition(), metadata.offset());
                } else {
                    logger.error("Producer error: {}", exception.getMessage(), exception);
                }
            });
        }

        producer.close();
    }
}