package com.octopx.zookeeper.embed;

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

public class ZooKeeperLocal {
	private ZooKeeperServerMain zooKeeperServer;
	
	public ZooKeeperLocal(Properties zkProperties) {
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(zkProperties);
		} catch (IOException | ConfigException e) {
			e.printStackTrace();
		}
		
		zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		
		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					System.out.println("ZooKeeper Failed");
					e.printStackTrace();
				}
			}
		}.start();
	}
}
