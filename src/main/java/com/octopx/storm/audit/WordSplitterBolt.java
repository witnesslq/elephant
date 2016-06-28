package com.octopx.storm.audit;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by yuyang on 16/6/28.
 */
public class WordSplitterBolt extends BaseRichBolt {
    private static final Logger logger = Logger.getLogger(WordSplitterBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        logger.info("RECV[kafka -> splitter] " + line);
        String[] words = line.split("\\s+");
        for (String word : words) {
            logger.info("EMIT[splitter -> counter] " + word);
            collector.emit(input, new Values(word, 1));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
