package com.octopx.storm.xlog;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XlogKafkaSpoutTopology {
    public static final Logger LOG = LoggerFactory.getLogger(XlogKafkaSpoutTopology.class);

    private final BrokerHosts brokerHosts;

    public XlogKafkaSpoutTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology(String topic) {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "", "xlog_" + topic);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 2).setNumTasks(8);
        builder.setBolt("SplitBolt", new SplitSentence(), 1).setNumTasks(2).shuffleGrouping("KafkaSpout");
        builder.setBolt("XlogBolt", new XlogBolt(), 4).setNumTasks(8).fieldsGrouping("SplitBolt", new Fields("ip"));
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            System.out.println("Usage:storm jar target/storm-xlog-***-jar-with-dependencies.jar  storm.xlog.XlogKafkaSpoutTopology configure_file_path");
            System.exit(0);
        }
        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("configure file " + args[0] + "not exist!");
            System.exit(0);
        }

        InputStream is = new FileInputStream(file);
        Properties prop = new Properties();
        prop.load(is);
        Config config = new Config();
        for (Object key : prop.keySet()) {
            config.put((String) key, prop.get(key));
        }
        is.close();
        String kafkaZk = (String) config.get("xlog.zookeeper.server");
        String nimbusIp = (String) config.get("xlog.nimbus.host");
        String topic = (String) config.get("xlog.kafka.topic.name");
        String debug = (String) config.get("xlog.debug");

        config.put(Config.TOPOLOGY_DEBUG, debug.toLowerCase().equals("true") ? true : false);
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
        config.setNumWorkers(1);
        config.setMaxTaskParallelism(10);
        config.setMaxSpoutPending(10000);
        config.put(Config.NIMBUS_HOST, nimbusIp);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(kafkaZk));

        XlogKafkaSpoutTopology XlogkafkaSpoutTopology = new XlogKafkaSpoutTopology(kafkaZk);
        StormTopology stormTopology = XlogkafkaSpoutTopology.buildTopology(topic);
        StormSubmitter.submitTopology(topic, config, stormTopology);
    }
}
