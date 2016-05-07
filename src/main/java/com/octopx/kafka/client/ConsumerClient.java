package com.octopx.kafka.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerClient {
	
//	public static void main(String[] args) throws IOException {
//		// Automatic Offset Committing
//		// This example demonstrates a simple usage of Kafka's consumer api that relying on automatic offset committing.
//		Properties props = new Properties();
//		//props.load(new FileInputStream("classpath:kafka/client.properties"));	//加载Properties文件，这种方式貌似不行
//		props.load(ConsumerClient.class.getClassLoader().getResourceAsStream("kafka/consumer.properties"));
//		
//		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//		consumer.subscribe(Arrays.asList("vmstat2"));
//		
//		while (true) {
//			ConsumerRecords<String, String> records = consumer.poll(100);
//			for (ConsumerRecord<String, String> record : records) {
//				System.out.printf("partition = %d, offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
//				System.out.println();
//			}
//		}
//	}
	
	public void automaticOffsetCommitting() {
		Properties props = new Properties();
		//props.load(new FileInputStream("classpath:kafka/client.properties"));	//加载Properties文件，这种方式貌似不行
		try {
			props.load(ConsumerClient.class.getClassLoader().getResourceAsStream("kafka/consumer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// ensuring the auto commit option has open
		String enableAutoCommit = "enable.auto.commit";
		if (props.getProperty(enableAutoCommit).equals("false")) {
			props.setProperty(enableAutoCommit, "true");
		}
		
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