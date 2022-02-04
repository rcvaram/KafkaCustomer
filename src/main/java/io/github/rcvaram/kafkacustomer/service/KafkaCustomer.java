package io.github.rcvaram.kafkacustomer.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaCustomer<K, V> {
    private final Consumer<K, V> consumer;
    private final ReentrantLock reentrantLock;
    private final int maxPollSeconds;
    private final int lockTimeOut;

    public KafkaCustomer(Consumer<K, V> consumer, int maxPollSeconds, int lockTimeOut) {
        this.consumer = consumer;
        this.maxPollSeconds = maxPollSeconds;
        this.lockTimeOut = lockTimeOut;
        this.reentrantLock = new ReentrantLock(true);
    }

    public ConsumerRecord<K, V> get(String topic, int partition, long topicOffset) throws InterruptedException, WakeupException {
        ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(topic, partition, -1, null, null);
        final boolean isAvailable = reentrantLock.tryLock(lockTimeOut, TimeUnit.SECONDS);
        if (isAvailable) {
            try {
                final Map<TopicPartition, Long> topicOffsetMap = Collections.singletonMap(new TopicPartition(topic, partition), topicOffset);
                consumer.assign(topicOffsetMap.keySet());
                topicOffsetMap.forEach(consumer::seek);
                consumer.resume(topicOffsetMap.keySet());
                final ConsumerRecords<K, V> poll = consumer.poll(Duration.ofSeconds(maxPollSeconds));
                for (ConsumerRecord<K, V> kvConsumerRecord : poll) {
                    if (kvConsumerRecord.offset() == topicOffset) {
                        consumerRecord = kvConsumerRecord;
                    }
                }
                consumer.commitSync();
                consumer.pause(topicOffsetMap.keySet());
            } finally {
                reentrantLock.unlock();
            }
        } else {
            System.out.println(MessageFormat.format("Lock is not available, thread already waited for {0} seconds.", lockTimeOut));
        }
        return consumerRecord;
    }


    public Map<TopicPartition, ConsumerRecord<K, V>> get(Map<TopicPartition, Long> topicOffset) throws InterruptedException, WakeupException {
        Map<TopicPartition, ConsumerRecord<K, V>> records = new HashMap<>();
        final boolean isAvailable = reentrantLock.tryLock(lockTimeOut, TimeUnit.SECONDS);
        if (isAvailable) {
            try {
                consumer.assign(topicOffset.keySet());
                topicOffset.forEach(consumer::seek);
                consumer.resume(topicOffset.keySet());
                final ConsumerRecords<K, V> poll = consumer.poll(Duration.ofSeconds(maxPollSeconds));
                for (ConsumerRecord<K, V> kvConsumerRecord : poll) {
                    final TopicPartition topicPartition = new TopicPartition(kvConsumerRecord.topic(), kvConsumerRecord.partition());
                    if (topicOffset.containsKey(topicPartition)) {
                        final Long offset = topicOffset.get(topicPartition);
                        if (offset == kvConsumerRecord.offset()) {
                            records.put(topicPartition, kvConsumerRecord);
                        }
                    }
                }
                consumer.commitSync();
                consumer.pause(topicOffset.keySet());
            } finally {
                reentrantLock.unlock();
            }
        } else {
            System.out.println(MessageFormat.format("Lock is not available, thread already waited for {0} seconds.", lockTimeOut));
        }
        return records;
    }

}
