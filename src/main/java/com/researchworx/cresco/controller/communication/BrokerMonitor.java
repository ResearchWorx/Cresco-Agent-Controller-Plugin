package com.researchworx.cresco.controller.communication;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;

class BrokerMonitor implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private String agentPath;
	private NetworkConnector bridge;
	public boolean MonitorActive;

	public BrokerMonitor(Launcher plugin, String agentPath) {
		this.logger = new CLogger(BrokerMonitor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
		this.agentPath = agentPath;
	}

	public void shutdown() {
		stopBridge(); //kill bridge
		MonitorActive = false;
	}

	public boolean connectToBroker(String brokerAddress, String brokerUserName, String brokerPassword) {
	    logger.trace("BrokerAddress: " + brokerAddress + " brokerUserName: " + brokerUserName + " brokerPassword:" + brokerPassword);
		boolean isConnected = false;
		try {
			if((InetAddress.getByName(brokerAddress) instanceof Inet6Address)) {
				brokerAddress = "[" + brokerAddress + "]";
			}
			bridge = this.plugin.getBroker().AddNetworkConnector(brokerAddress, brokerUserName, brokerPassword);
			bridge.start();
            logger.trace("Starting Bridge: " + bridge.getBrokerName() + " brokerAddress: " + brokerAddress);
			int connect_count = 0;
			while((connect_count++ < 10) && !bridge.isStarted()) {
				Thread.sleep(1000);
                logger.trace("Wating on Bridge to Start: " + bridge.getBrokerName());

            }
            logger.trace("Bridge.isStarted: " + bridge.isStarted() + " brokerName: " + bridge.getBrokerName());
            //
            //Send a message
            /*
            List<ActiveMQDestination> dest = bridge.getDynamicallyIncludedDestinations();
            for(ActiveMQDestination ades : dest) {
                logger.trace("MQDEST: " + ades.getPhysicalName() + " " + ades.getQualifiedName() + " " + ades.isQueue());
            }
            Set<ActiveMQDestination> dests = bridge.getDurableDestinations();
            for(ActiveMQDestination ades : dests) {
                logger.trace("MQDESTS: " + ades.getPhysicalName() + " " + ades.getQualifiedName() + " " + ades.isQueue());
            }
            */
            //


            if (connect_count >= 10 && !bridge.isStarted()) {
				throw new Exception("Failed to start bridge after 10 attempts. Aborting.");
			}
            logger.trace("Bridge isConnected 0 : " + isConnected);

            //THIS SHOULD NOT BE HERE
            isConnected = true;
            //THIS SHOULD NOT BE HERE
            logger.trace("Bridge isConnected 1 : " + isConnected);

            connect_count = 0;
			while((connect_count++ < 10) && !isConnected) {
                logger.trace("ActiveBridge Count: " + bridge.activeBridges().size());

                for(NetworkBridge b : bridge.activeBridges()) {
                    String remoteBroker = b.getRemoteBrokerName();

                    logger.trace("RemoteBroker: " + b.getRemoteBrokerName() + " " + b.getRemoteAddress() + " " + b.getLocalAddress() + " " + b.getLocalBrokerName());
                    if(remoteBroker != null) {
                        logger.trace("RemoteBroker: " + remoteBroker + " agentPath: " + agentPath);
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
		    /*
            while(this.plugin.getBrokeredAgents().get(agentPath).brokerStatus == BrokerStatusType.STARTING) {
                logger.trace("Waiting on agentpath: " + agentPath + " brokerstatus: " + this.plugin.getBrokeredAgents().get(agentPath).brokerStatus.toString());
                Thread.sleep(1000);
            }
		    */
            /*
			String brokerAddress = this.plugin.getBrokeredAgents().get(agentPath).activeAddress;
			if (connectToBroker(brokerAddress)) { //connect to broker
				MonitorActive = true;
				this.plugin.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.ACTIVE;
			}
            */

            String brokerAddress = this.plugin.getBrokeredAgents().get(agentPath).activeAddress;
            String brokerUsername = this.plugin.getBrokeredAgents().get(agentPath).brokerUsername;
            String brokerPassword = this.plugin.getBrokeredAgents().get(agentPath).brokerPassword;
            if (connectToBroker(brokerAddress,brokerUsername,brokerPassword)) { //connect to broker
                MonitorActive = true;
                this.plugin.getBrokeredAgents().get(agentPath).brokerStatus = BrokerStatusType.ACTIVE;
            }
            while (MonitorActive) {
				MonitorActive = false;
				for (NetworkBridge b : bridge.activeBridges()) {
				    logger.trace("Check Broker Name: " + b.getRemoteBrokerName() + " for agentPath: " + agentPath);

                    //if (b.getRemoteBrokerName().equals(agentPath)) {
					    MonitorActive = true;
					//}

                }
				Thread.sleep(5000);
			}
			logger.trace("agentpath: " + agentPath + " is being shutdown");
			shutdown();
		} catch(Exception ex) {
			logger.error("Run {}", ex.getMessage());
            logger.error(ex.getStackTrace().toString());
		}
	}
}