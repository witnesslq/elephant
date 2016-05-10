package com.octopx.storm.spout;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random random;
	private AtomicInteger counter;
	
	private static String[] sentences = new String[] {
		"edi:I'm happy", "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous"
	};

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
		this.counter = new AtomicInteger();
	}
	
	@Override
	public void nextTuple() {
		String toSay = sentences[random.nextInt(sentences.length)];
		int msgId = this.counter.getAndIncrement();
		this.collector.emit(new Values(toSay), msgId);	//collector发射tuple，基本元数据是使用Tuple承载的，它要求实现类必须能够序列化
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));	//在整个stream中定义一个别名，这个值必须在整个topology唯一
	}

	@Override
	public void ack(Object msgId) {
		System.err.println("ack " + msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		System.err.println("fail " + msgId);
	}
}
