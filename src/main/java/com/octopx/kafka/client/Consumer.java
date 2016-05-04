package com.octopx.kafka.client;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import static kafka.consumer.Consumer.createJavaConsumerConnector;

public class Consumer {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	
	public Consumer(String zookeeper, String groupId, String topic) {
		ConsumerConfig consumerConfig = createConsumerConfig(zookeeper, groupId);
		this.consumer = createJavaConsumerConnector(consumerConfig);
		this.topic = topic;
	}
	
	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		
		return new ConsumerConfig(props);
	}
	
	public void shutdown() {
		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}
}
