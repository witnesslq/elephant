package com.octopx.storm.audit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.UUID;

/**
 * Created by yuyang on 16/6/28.
 */
public class AuditTopology {
    private final static String zkHosts = "192.168.1.90:2181,192.168.1.91:2181,192.168.1.92:2181";
    private final static String topicName = "audit";
    private final static String zkRoot = "/" + topicName;

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts(zkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, zkRoot, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("kafka-spout", kafkaSpout, 5);
        builder.setBolt("line-printer", new PrintBolt(), 1).shuffleGrouping("kafka-spout");
//        builder.setBolt("word-splitter", new WordSplitterBolt(), 2).shuffleGrouping("kafka-spout");
//        builder.setBolt("word-counter", new WordCounterBolt()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        String name = AuditTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(name, conf, builder.createTopology());
        }
    }
}