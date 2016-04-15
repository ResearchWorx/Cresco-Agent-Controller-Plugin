package com.researchworx.cresco.controller.communication;

import java.net.Inet6Address;
import java.net.InetAddress;

import com.researchworx.cresco.controller.core.Launcher;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BrokerMonitor implements Runnable {
	private Launcher plugin;
	private String agentPath;
	private NetworkConnector bridge;
	public boolean MonitorActive;

	private static final Logger logger = LoggerFactory.getLogger(BrokerMonitor.class);

	public BrokerMonitor(Launcher plugin, String agentPath) {
		this.plugin = plugin;
		this.agentPath = agentPath;
	}

	public void shutdown() {
		stopBridge(); //kill bridge
		MonitorActive = false;
	}

	public boolean connectToBroker(String brokerAddress) {
		boolean isConnected = false;
		try {
			if((InetAddress.getByName(brokerAddress) instanceof Inet6Address)) {
				brokerAddress = "[" + brokerAddress + "]";
			}
			bridge = this.plugin.getBroker().AddNetworkConnector(brokerAddress);
			bridge.start();
			int connect_count = 0;
			while((connect_count++ < 10) && !bridge.isStarted()) {
				Thread.sleep(1000);
			}
			if (connect_count >= 10 && !bridge.isStarted()) {
				throw new Exception("Failed to start bridge after 10 attempts. Aborting.");
			}
			connect_count = 0;
			while((connect_count++ < 10) && !isConnected) {
				for(NetworkBridge b : bridge.activeBridges()) {
					String remoteBroker = b.getRemoteBrokerName();
					if(remoteBroker != null) {
						if(remoteBroker.equals(agentPath)) {
	    					isConnected = true;
	    				}
					}
					Thread.sleep(1000);
				}
			}
		} catch(Exception ex) {
			logger.error(getClass().getName() + " connectToBroker Error " + ex.toString());
		}
		return isConnected;
	}
	  
	public void stopBridge() {
		logger.trace("Stopping Bridge : " + agentPath);
		try {
			this.plugin.getBroker().removeNetworkConnector(bridge);
		} catch (Exception e) {
			logger.error("stopBridge {}", e.getMessage());
		}
		this.plugin.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.FAILED;
	}
	  
	public void run() {
		try {
			String brokerAddress = this.plugin.getBrokeredAgents().get(agentPath).activeAddress;
			if (connectToBroker(brokerAddress)) { //connect to broker
				MonitorActive = true;
				this.plugin.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.ACTIVE;
			}
			while (MonitorActive) {
				MonitorActive = false;
				for (NetworkBridge b : bridge.activeBridges()) {
					if (b.getRemoteBrokerName().equals(agentPath)) {
						MonitorActive = true;
					}
				}
				Thread.sleep(5000);
			}
			shutdown();
		} catch(Exception ex) {
			logger.error("Run {}", ex.getMessage());
		}
	}
}