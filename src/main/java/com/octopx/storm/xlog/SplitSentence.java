package com.octopx.storm.xlog;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by yuyang on 16/7/4.
 */
public class SplitSentence extends BaseBasicBolt {
    private int thisTaskId = 0;

    public void prepare(Map stormConf, TopologyContext context) {
        thisTaskId = context.getThisTaskId();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String line = tuple.getString(0);
        String[] lineArr = StringUtils.split(line.substring(0, 20), " ");
        //System.out.println(lineArr[0]);
        collector.emit(new Values(lineArr[0], line));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "line"));
    }
}
