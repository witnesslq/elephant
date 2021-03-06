package com.octopx.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerClient {

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
		consumer.subscribe(Arrays.asList("audit"));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("partition = %d, offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
				System.out.println();
			}
		}
	}
}