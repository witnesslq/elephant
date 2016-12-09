package com.octopx.storm.basic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class ExclaimBasicTopo {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new RandomSpout(), 2).setNumTasks(4);	//设置Executor数和Task数
		builder.setBolt("exclaim", new ExclaimBasicBolt(), 2).shuffleGrouping("spout");
		builder.setBolt("print", new PrintBolt(), 2).shuffleGrouping("exclaim");


		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(2);	//设置Worker数增加Workers：config.setNumWorkers(2);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local_test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("local_test");
			cluster.shutdown();
		}
	}
}
