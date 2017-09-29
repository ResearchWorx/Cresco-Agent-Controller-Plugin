package com.researchworx.cresco.controller.communication;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryStatic;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryType;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.network.NetworkConnector;

import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

public class ActiveBrokerManager implements Runnable  {
	private Launcher plugin;
	private CLogger logger;
	private Timer timer;
	public ActiveBrokerManager(Launcher plugin) {
		this.logger = new CLogger(ActiveBrokerManager.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);

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
		//logger.error("addBroker: agentPath = " + agentPath + " status 0 = " + ba.brokerStatus.toString());
		if(ba.brokerStatus == BrokerStatusType.INIT) {
			//Fire up new thread.
			ba.setStarting();
		}
		/*
		logger.error("addBroker: agentPath = " + agentPath + " status 1 = " + ba.brokerStatus.toString());
		if(ba.brokerStatus == BrokerStatusType.STARTING) {
			ba.setActive();
		}
		logger.error("addBroker: agentPath = " + agentPath + " status 2 = " + ba.brokerStatus.toString());
		*/
	}
	public void run() {
		logger.info("Active Broker Manager started");
		this.plugin.setActiveBrokerManagerActive(true);
		while(this.plugin.isActiveBrokerManagerActive()) {
			try {
				MsgEvent cb = this.plugin.getIncomingCanidateBrokers().take();
				if(cb != null) {

					String agentIP = cb.getParam("dst_ip");
					if(!this.plugin.isLocal(agentIP)) { //ignore local responses

						boolean addBroker = false;
						String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
						logger.trace("Trying to connect to: " + agentPath);
						//logger.trace(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;
	 		      
						BrokeredAgent ba = null;
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
								logger.trace("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
							}
							logger.trace("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
						} else {
						    //This might not work everwhere
                            String cbrokerAddress = null;
                            String cbrokerValidatedAuthenication = null;
                            cbrokerAddress = cb.getParam("dst_ip");
                            cbrokerValidatedAuthenication = cb.getParam("validated_authenication");

							if(cbrokerValidatedAuthenication != null) {

								DiscoveryStatic ds = new DiscoveryStatic(plugin);
								List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.REGION, plugin.getConfig().getIntegerParam("discovery_static_region_timeout", 10000), cbrokerAddress, true);
                				for(MsgEvent cme : certDiscovery) {
                					if(cbrokerAddress.equals(cme.getParam("dst_ip"))) {
										String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
										ba = new BrokeredAgent(this.plugin, agentPath, cbrokerAddress, tmpAuth[0], tmpAuth[1]);
										this.plugin.getBrokeredAgents().put(agentPath, ba);
										addBroker = true;
										logger.trace("BA NEW ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
									}
								}

							}
						}
						//try and connect
						if(addBroker && !this.plugin.isReachableAgent(agentPath)) {
                            addBroker(agentPath);
                            int count = 0;
                            //while(!this.plugin.isReachableAgent(agentPath)) {

                            	/*
                            	MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");
								sme.setParam("src_region", this.plugin.getRegion());
								sme.setParam("src_agent", this.plugin.getAgent());
								String[] regionAgent = agentPath.split("_");
								sme.setParam("dst_region",regionAgent[0]);
								sme.setParam("dst_agent",regionAgent[1]);
								plugin.sendMsgEvent(sme);
								*/

								logger.trace("Waiting on Broker : " + agentPath + " remote_ip: " + agentIP + " count:" + count);
								logger.trace("Status : " + ba.brokerStatus.toString() + " URI : " + ba.URI + " Address : " + ba.activeAddress);
								logger.trace("isReachable : " + this.plugin.isReachableAgent(agentPath));
								Thread.sleep(1000);
                                count++;
                            //}
						}
						else {
                            logger.trace("Not Adding Broker : " + agentPath + " remote_ip: " + agentIP);
                        }
					}
		  			//Thread.sleep(500); //allow HM to catch up
			  	}
			} catch (Exception ex) {
				logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
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
		    for (Entry<String, BrokeredAgent> entry : plugin.getBrokeredAgents().entrySet()) {
				//logger.trace(entry.getKey() + "/" + entry.getValue());
				BrokeredAgent ba = entry.getValue();
				if(ba.brokerStatus == BrokerStatusType.FAILED) {
					//logger.trace("stopping agentPath: " + ba.agentPath);
		    		ba.setStop();
		    		logger.info("Cleared agentPath: " + ba.agentPath);

		    		if((plugin.getGlobalControllerPath()) != null && (plugin.getGlobalControllerPath().equals(ba.agentPath))) {
                        logger.info("Clearing Global Controller Path " +ba.agentPath);
                        plugin.setGlobalControllerPath(null);
                    }

                    plugin.getBrokeredAgents().remove(entry.getKey());//remove agent
				}
                logger.trace("Brokered Agents: " + ba.agentPath);
			}
		}
	}
}