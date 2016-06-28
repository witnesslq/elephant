package com.octopx.storm.basic.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ExclaimBasicBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = (String) tuple.getValue(0);
		String out = sentence.concat("!");
		collector.emit(new Values(out));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("exec_sentence"));
	}
	
}
