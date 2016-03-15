package ActiveMQ;

import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;
import shared.MsgEvent;

public class ActiveBrokerManager implements Runnable  {
	//private MulticastSocket socket;
	private Timer timer;
	private static final Logger logger = LoggerFactory.getLogger(ActiveBrokerManager.class);
	public ActiveBrokerManager() {
		logger.debug("Active Broker Manger initialized");
		timer = new Timer();
	    //timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 300000);//remote 
		timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 15000);//remote 
	}
	  
	public void shutdown() {
		logger.debug("Active Broker Manager shutdown initialized");
	}

	public void addBroker(String agentPath) {
		BrokeredAgent ba = PluginEngine.brokeredAgents.get(agentPath);
		if(ba.brokerStatus == BrokerStatusType.INIT) {
			//Fire up new thread.
			ba.setStarting();
		}
	}
	public void run() {
		logger.info("Active Broker Manager started");
		PluginEngine.ActiveBrokerManagerActive = true;
		while(PluginEngine.ActiveBrokerManagerActive) {
			try {
				MsgEvent cb = PluginEngine.incomingCanidateBrokers.poll();
				if(cb != null) {
					String agentIP = cb.getParam("dst_ip");
					if(!PluginEngine.isLocal(agentIP)) { //ignore local responses
						boolean addBroker = false;
						String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
						//System.out.println(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;
	 		      
						BrokeredAgent ba;
						if(PluginEngine.brokeredAgents.containsKey(agentPath)) {
							ba = PluginEngine.brokeredAgents.get(agentPath);
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
							ba = new BrokeredAgent(agentIP,agentPath);
							PluginEngine.brokeredAgents.put(agentPath, ba);
							addBroker = true;
							//System.out.println("BA NEW ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
						}
						//try and connect
						if(addBroker && !PluginEngine.isReachableAgent(agentPath)) {
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
		private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);

		public void run() {
			for (Entry<String, BrokeredAgent> entry : PluginEngine.brokeredAgents.entrySet()) {
				//System.out.println(entry.getKey() + "/" + entry.getValue());
				BrokeredAgent ba = entry.getValue();
				if(ba.brokerStatus == BrokerStatusType.FAILED) {
					//System.out.println("stopping agentPath: " + ba.agentPath);
		    		ba.setStop();
		    		logger.info("Cleared agentPath: " + ba.agentPath);
		    		PluginEngine.brokeredAgents.remove(entry.getKey());//remove agent
				}
			}
		}
	}
}