# kafka sample

## link 

### cheat-sheet
https://www.redpanda.com/guides/kafka-tutorial-kafka-cheat-sheet

### basics
- https://docs.confluent.io/kafka/design/delivery-semantics.html

### EOS
- https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/
- https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
- https://www.confluent.io/kafka-summit-sf17/exactly-once-stream-processing-with-kafka-streams/
- https://www.lixueduan.com/posts/kafka/10-exactly-once-impl/

### kafka 4.0
- https://www.confluent.io/blog/introducing-apache-kafka-4-0/
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka#KIP932:QueuesforKafka-Sharegroupmembership

## Start with docker
```bash
docker run --name kafka1 -d -p 9092:9092 apache/kafka:4.0.0

docker exec -it kafka1 /bin/bash
cd /opt/kafka/bin/
```

## command

kafka-topics
```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list

./kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1

./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-topic

./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-topic
```

kafka-consumer-groups
```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group simple-eos-app
```

kafka-console-consumer
```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

## EOS
- seems by default `read_uncommitted` in all consumer.

