package com.octopx.apache.kafka;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerClient {
	
	public void produce(long events) {

		Properties props = new Properties();
		try {
			props.load(ProducerClient.class.getClassLoader().getResourceAsStream("kafka/producer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		for (long i = 0; i < events; i++) {
			Random random = new Random();
			long runtime = new Date().getTime();
			String ip = "192.168.2." + random.nextInt(255);
			String item = runtime + ",www.octopx.com," + ip;
			KeyedMessage<String, String> message = new KeyedMessage<String, String>("page_visits", ip, item);
			producer.send(message);
		}
		
		producer.close();
	}
}
