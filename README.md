# KafkaCustomer

This is not a traditional consumer of Spring Kafka. This is a simple kafka customer who can get only specifc offset message of kafka topics.
This kafka customer is handy in testing the messages of kafka and, if someone needs to read a particular offset message only. 

```
<dependency>
  <groupId>io.github.rcvaram</groupId>
  <artifactId>KafkaCustomer</artifactId>
  <version>1.0</version>
</dependency>
```

## How to use
 Create a KafkaCustomer with your SpringKafkaConsumerFactory as follow.
```
ConsumerFactory<String, String> consumerFactory;            
KafkaCustomer<String, String> kafkaCustomer = new KafkaCustomer<>(consumerFactory, 1000, 5);
```
Create the topicPartition and offset Mapper 
```
 TopicPartition topicPartitionZero = new TopicPartition("test",1);
 TopicPartition topicPartitionOne = new TopicPartition("test",0);
 Map<TopicPartition, Long> map= new HashMap();
 map.put(topicPartitionZero, 5);
 map.put(topicPartitionOne, 6);
 Map<TopicPartition, ConsumerRecord<String, String>> OffsetValueMapper = kafkaCustomer.get(map);
 ```
 
 

 
 
