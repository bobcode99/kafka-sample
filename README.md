
# kafka

## docker

```bash
./kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1

./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-topic

./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-topic

./kafka-console-consumer.sh --bootstrap-server --topic test-topic --from-beginning
```