package com.octopx.storm.audit;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yuyang on 16/6/29.
 */
public class PrintBolt extends BaseRichBolt {
    private final static Logger logger = Logger.getLogger(PrintBolt.class);
    private AtomicInteger ai;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        //logger.error("++" + line);
        Grok grok = new Grok();
        try {
            grok.compile("%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}");
            Match gm = grok.match(line);
            gm.captures();
            System.out.println("+++" + gm.toJson());
        } catch (GrokException e) {
            e.printStackTrace();
        }

        if (ai == null) {
            ai = new AtomicInteger();
        }
        ai.addAndGet(1);
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        logger.error("+++++++ count = " + ai.get());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
