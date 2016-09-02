package com.researchworx.cresco.controller.communication;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

public class ActiveBrokerManager implements Runnable  {
	private Launcher plugin;
	private CLogger logger;
	private Timer timer;
	public ActiveBrokerManager(Launcher plugin) {
		this.logger = new CLogger(ActiveBrokerManager.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);

		logger.debug("Active Broker Manger initialized");
		this.plugin = plugin;
		timer = new Timer();
		timer.scheduleAtFixedRate(new BrokerWatchDog(logger), 500, 15000);//remote
	}
	  
	public void shutdown() {
		logger.debug("Active Broker Manager shutdown initialized");
	}

	public void addBroker(String agentPath) {
		BrokeredAgent ba = this.plugin.getBrokeredAgents().get(agentPath);
		if(ba.brokerStatus == BrokerStatusType.INIT) {
			//Fire up new thread.
			ba.setStarting();
		}
	}
	public void run() {
		logger.info("Active Broker Manager started");
		this.plugin.setActiveBrokerManagerActive(true);
		while(this.plugin.isActiveBrokerManagerActive()) {
			try {
				MsgEvent cb = this.plugin.getIncomingCanidateBrokers().poll();
				if(cb != null) {
					String agentIP = cb.getParam("dst_ip");
					if(!this.plugin.isLocal(agentIP)) { //ignore local responses
						boolean addBroker = false;
						String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
						//System.out.println(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;
	 		      
						BrokeredAgent ba;
						if(this.plugin.getBrokeredAgents().containsKey(agentPath)) {
							ba = this.plugin.getBrokeredAgents().get(agentPath);
							//add ip to possible list
							if(!ba.addressMap.containsKey(agentIP)) {
								ba.addressMap.put(agentIP,BrokerStatusType.INIT);
							}
							//reset status if needed
							if((ba.brokerStatus.equals(BrokerStatusType.FAILED) || (ba.brokerStatus.equals(BrokerStatusType.STOPPED)))) {
								ba.activeAddress = agentIP;
								ba.brokerStatus = BrokerStatusType.INIT;
								addBroker = true;
								//System.out.println("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
							}
							//System.out.println("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
						} else {
							ba = new BrokeredAgent(this.plugin, agentIP,agentPath);
							this.plugin.getBrokeredAgents().put(agentPath, ba);
							addBroker = true;
							//System.out.println("BA NEW ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
						}
						//try and connect
						if(addBroker && !this.plugin.isReachableAgent(agentPath)) {
							addBroker(agentPath);
						}
					}
		  			//Thread.sleep(500); //allow HM to catch up
			  	} else {
					Thread.sleep(1000);
				}
			} catch (Exception ex) {
				logger.error("Run {}", ex.getMessage());
			}
		}
		timer.cancel();
		logger.debug("Broker Manager has shutdown");
	}

	class BrokerWatchDog extends TimerTask {
		//private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);
        private CLogger logger;

        public BrokerWatchDog(CLogger logger) {
            this.logger = logger;
        }
		public void run() {
		    logger.error("COME AT ME");
            for (Entry<String, BrokeredAgent> entry : plugin.getBrokeredAgents().entrySet()) {
				//System.out.println(entry.getKey() + "/" + entry.getValue());
				BrokeredAgent ba = entry.getValue();
				if(ba.brokerStatus == BrokerStatusType.FAILED) {
					//System.out.println("stopping agentPath: " + ba.agentPath);
		    		ba.setStop();
		    		logger.info("Cleared agentPath: " + ba.agentPath);
		    		plugin.getBrokeredAgents().remove(entry.getKey());//remove agent
				}
                logger.trace("Brokered Agents: " + ba.agentPath);
			}
		}
	}
}