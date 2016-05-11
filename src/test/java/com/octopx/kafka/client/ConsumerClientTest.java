package com.octopx.kafka.client;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.octopx.kafka.client.ConsumerClient;

public class ConsumerClientTest {
	@Test
	public void testAutomaticOffsetCommitting() {
		ConsumerClient consumer = new ConsumerClient();
		consumer.automaticOffsetCommitting();
		assertTrue(true);
	}
}
