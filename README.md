
# kafka

- link
- https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/
- https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
- https://www.lixueduan.com/posts/kafka/10-exactly-once-impl/
- 

## docker

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list

./kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1

./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-topic

./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-topic


./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group simple-eos-app
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```
