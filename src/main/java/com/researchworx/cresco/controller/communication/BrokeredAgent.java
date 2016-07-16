package com.researchworx.cresco.controller.communication;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {
	public Map<String,BrokerStatusType> addressMap;
	public BrokerStatusType brokerStatus;
	public String activeAddress;
	public String agentPath;
	public BrokerMonitor bm;
	private Launcher plugin;
	private CLogger logger;

	public BrokeredAgent(Launcher plugin, String activeAddress, String agentPath) {
		this.logger = new CLogger(BrokeredAgent.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		logger.debug("Initializing: " + agentPath + " address: " + activeAddress);
		System.out.print("Name of Agent to message [q to quit]: ");
		this.plugin = plugin;
		this.bm = new BrokerMonitor(plugin, agentPath);
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<>();
		this.addressMap.put(activeAddress, BrokerStatusType.INIT);
	}

	public void setStop() {
		if(bm.MonitorActive) {
			bm.shutdown();
		}
		while(bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("setStop {}", e.getMessage());
			}
		}
		brokerStatus = BrokerStatusType.STOPPED;
		logger.debug("setStop : Broker STOP");
	}

	public void setStarting() {
		brokerStatus = BrokerStatusType.STARTING;
		addressMap.put(activeAddress, BrokerStatusType.STARTING);
		if(bm.MonitorActive) {
			bm.shutdown();
		}
		bm = new BrokerMonitor(plugin, agentPath);
		new Thread(bm).start();
		while(!bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("setStarting {}", e.getMessage());
			}
		}
	}

	public void setActive() {
		brokerStatus = BrokerStatusType.ACTIVE;
		addressMap.put(activeAddress, BrokerStatusType.ACTIVE);
	}
}