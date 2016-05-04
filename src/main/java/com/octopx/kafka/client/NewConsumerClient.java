package com.octopx.kafka.client;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NewConsumerClient {
	
	public static void main(String[] args) {
		// Automatic Offset Committing
		// This example demonstrates a simple usage of Kafka's consumer api that relying on automatic offset committing.
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.90:9092,192.168.1.91:9092,192.168.1.92:9092");
		props.put("group.id", "yangI");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("vmstat2"));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("partition = %d, offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
				System.out.println();
			}
		}
	}
}