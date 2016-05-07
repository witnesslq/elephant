package com.octopx.storm.topo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.octopx.storm.bolt.ExclaimBasicBolt;
import com.octopx.storm.bolt.PrintBolt;
import com.octopx.storm.spout.RandomSpout;


public class ExclaimBasicTopo {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new RandomSpout());
		builder.setBolt("exclaim", new ExclaimBasicBolt()).shuffleGrouping("spout");
		builder.setBolt("print", new PrintBolt()).shuffleGrouping("exclaim");
		
		Config conf = new Config();
		conf.setDebug(false);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);	//设置Worker数为3个
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local_test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("local_test");
			cluster.shutdown();
		}
	}
}
