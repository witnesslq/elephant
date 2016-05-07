package com.octopx.storm.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random random;
	private static String[] sentences = new String[] {
		"edi:I'm happy", "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous"
	};

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}
	
	@Override
	public void nextTuple() {
		String toSay = sentences[random.nextInt(sentences.length)];
		this.collector.emit(new Values(toSay));	//collector发射tuple，基本元数据是使用Tuple承载的，它要求实现类必须能够序列化
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));	//在整个stream中定义一个别名，这个值必须在整个topology唯一
	}

}
