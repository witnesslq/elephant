package com.octopx.kafka.client;

import org.junit.Test;

import com.octopx.kafka.ProducerClient;

import static org.junit.Assert.*;

public class ProducerClientTest {

	@Test
	public void testProducer() {
		ProducerClient producerClient = new ProducerClient();
		producerClient.produce(1002);
		assertTrue(true);
	}
}