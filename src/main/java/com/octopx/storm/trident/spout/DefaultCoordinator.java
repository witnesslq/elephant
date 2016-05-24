package com.octopx.storm.trident.spout;

import java.io.Serializable;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;
import org.slf4j.LoggerFactory;


public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {
	private static final long serialVersionUID = 1L;
	//private static final Logger LOG = LoggerFactory.getILoggerFactory();

	@Override
	public void close() {
	}

	@Override
	public Long initializeTransaction(long txid, Long prevMetadata, Long arg2) {
		return null;
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void success(long txid) {
		
	}

}
