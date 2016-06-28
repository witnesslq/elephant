package com.octopx.storm.basic.trident.spout;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class DiagnosisEventSpout implements ITridentSpout<Long> {
	private static final long serialVersionUID = 1L;
	private BatchCoordinator<Long> coordinator = new DefaultCoordinator();
	private Emitter<Long> emitter = new DiagnosisEventEmitter();
	private static final Logger logger = LoggerFactory.getLogger(DiagnosisEventSpout.class);

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return coordinator;
	}

	@Override
	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return emitter;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("event");
	}
}
