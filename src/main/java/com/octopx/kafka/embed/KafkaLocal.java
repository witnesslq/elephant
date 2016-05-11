package com.octopx.kafka.embed;

import java.util.Properties;

import com.octopx.zookeeper.embed.ZooKeeperLocal;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class KafkaLocal {
	public KafkaServerStartable kafka;
	public ZooKeeperLocal zookeeper;
	
	public KafkaLocal(Properties kafkaProperties, Properties zkProperties) {
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
		
		// start local zookeeper
		System.out.println("starting local zookeeper...");
		zookeeper = new ZooKeeperLocal(zkProperties);
		System.out.println("local zookeeper started");
		
		// start local kafka broker
		kafka = new KafkaServerStartable(kafkaConfig);
		System.out.println("starting local kafka broker...");
		kafka.startup();
		System.out.println("local kafka broker started");
	}
	
	public void stop() {
		// stop local kafka broker
		System.out.println("stopping kafka...");
		kafka.shutdown();
		System.out.println("kafka stop");
	}
}
