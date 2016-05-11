package com.octopx.kafka.embed;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

public class KafkaLocalTest {
	public static KafkaLocal kafka;
	
	@BeforeClass
	public static void startKafka() {
		Properties kafkaProperties = new Properties();
		Properties zkProperties = new Properties();
		
		try {
			// load properties
			kafkaProperties.load(KafkaLocalTest.class.getClassLoader().getResourceAsStream("kafka_local.properties"));
			zkProperties.load(KafkaLocalTest.class.getClassLoader().getResourceAsStream("zk_local.properties"));
			
			// start kafka
			kafka = new KafkaLocal(kafkaProperties, zkProperties);
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSomething() {
		;
	}
}
