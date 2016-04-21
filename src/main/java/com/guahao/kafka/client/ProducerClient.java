package com.guahao.kafka.client;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerClient {
	
	public void produce(long events) {

		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.1.90:9092,192.168.1.91:9092,192.168.1.92:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.guahao.kafka.client.SimplePartitioner");
		props.put("request.required.acks", "1");
		
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
