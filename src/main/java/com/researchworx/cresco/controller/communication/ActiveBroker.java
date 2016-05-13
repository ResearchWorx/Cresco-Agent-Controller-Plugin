package com.researchworx.cresco.controller.communication;

import com.researchworx.cresco.controller.core.Launcher;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.security.*;
import org.apache.activemq.util.ServiceStopper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.sun.tools.internal.ws.wsdl.parser.Util.fail;

public class ActiveBroker {
	private static final Logger logger = LoggerFactory.getLogger(ActiveBroker.class);

	public BrokerService broker;
	public TransportConnector connector;


	public ActiveBroker(String brokerName, String brokerUserNameAgent, String brokerPasswordAgent) {
		logger.info("Broker initialized");
		try {
			if(portAvailable(32010)) {
				PolicyEntry entry = new PolicyEntry();
		        entry.setGcInactiveDestinations(true);
		        entry.setInactiveTimeoutBeforeGC(5000);
		        PolicyMap map = new PolicyMap();
		        map.setDefaultEntry(entry);

				broker = new BrokerService();
				broker.setUseShutdownHook(false);
				broker.setPersistent(false);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(2500);
				broker.setDestinationPolicy(map);
				broker.setUseJmx(false);

                //auth
                DefaultAuthorizationMap authMap = new DefaultAuthorizationMap();
                List<DestinationMapEntry> authEntries = new ArrayList();

                AuthorizationEntry authEntry = new AuthorizationEntry();
                authEntry.setGroupClass("org.apache.activemq.jaas.GroupPrincipal");
                authEntry.setQueue(">");
                authEntry.setRead("agents");
                authEntry.setWrite("agents");
                authEntries.add(authEntry);

                authMap.setAuthorizationEntries(authEntries);


                logger.info("PREAUTH0");

                //set auth username
                SimpleAuthenticationPlugin simpleAuthenticationPlugin = new SimpleAuthenticationPlugin();
                logger.info("PREAUTH0.0");

                simpleAuthenticationPlugin.setAnonymousAccessAllowed(false);
                logger.info("PREAUTH0.1");

                //AuthenticationUser autogenUser = new AuthenticationUser(brokerUserNameAgent,brokerPasswordAgent,"users,admins");
                AuthenticationUser autogenUser = new AuthenticationUser(brokerUserNameAgent,brokerPasswordAgent,"agents");
                logger.info("PREAUTH0.2");

                List<AuthenticationUser> users = new ArrayList<>();
                logger.info("PREAUTH0.3");

                users.add(autogenUser);
                logger.info("PREAUTH0.4");

                simpleAuthenticationPlugin.setUsers(users);
                //simpleAuthenticationPlugin

                logger.info("PREAUTH1");
                broker.setPlugins(new BrokerPlugin[]{simpleAuthenticationPlugin});
                logger.info("PREAUTH2");

                connector = new TransportConnector();

				connector.setUri(new URI("tcp://[::]:32010"));

				broker.addConnector(connector);
                logger.info("PREAUTH3");

                broker.start();
                logger.info("PREAUTH4");

                while(!broker.isStarted()) {
			    	Thread.sleep(1000);
                    logger.info("PREAUTH5");

                }
                logger.info("PREAUTH6");

            } else {
				System.out.println("Constructor : portAvailable(32010) == false");
			}
		} catch(Exception ex) {
			ex.printStackTrace();
			System.out.println("Init {}" + ex.getMessage());
		}
	}	

	public boolean isHealthy() {
		boolean isHealthy = false;
		try  {
			if(broker.isStarted()) {
				isHealthy = true;
			}
		} catch (Exception e) {
			logger.error("isHealthy {}", e.getMessage());
		}
		return isHealthy;
	}
	
	public void stopBroker() {
		try {
			connector.stop();
			ServiceStopper stopper = new ServiceStopper();
			//broker.getManagementContext().stop();
            broker.stopAllConnectors(stopper);
            broker.stop();

			while(!broker.isStopped()) {
				Thread.sleep(1000);
			}
			logger.debug("Broker has shutdown");
		} catch (Exception e) {
			logger.error("stopBroker {}", e.getMessage());
		}
		
	}

	public boolean removeNetworkConnector(NetworkConnector bridge) {
		boolean isRemoved = false;
		try {
			bridge.stop();
			while(!bridge.isStopped()) {
				Thread.sleep(1000);
			}
			broker.removeNetworkConnector(bridge);
			isRemoved = true;
		}
		catch(Exception ex) {
			logger.error("removeNetworkConnector {}", ex.getMessage());
		}
		return isRemoved; 
		
	}

	public NetworkConnector AddNetworkConnector(String URI) {
		NetworkConnector bridge = null;
		try {
			bridge = broker.addNetworkConnector(new URI("static:tcp://" + URI + ":32010"));
			//RandomString rs = new RandomString(5);
			bridge.setUserName("cody");
            bridge.setPassword("cody");
			bridge.setName(java.util.UUID.randomUUID().toString());
			bridge.setDuplex(true);
			bridge.setDynamicOnly(true);
			bridge.setPrefetchSize(1);
		} catch(Exception ex) {
			logger.error("AddNetworkConnector {}", ex.getMessage());
		}
		return bridge;
	}
	
	public void AddTransportConnector(String URI) {
		try {
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));

			this.broker.addConnector(connector);
			this.broker.startTransportConnector(connector);
		} catch(Exception ex) {
			logger.error("AddTransportConnector {}", ex.getMessage());
		}
	}
	
	public boolean portAvailable(int port) {
		if (port < 0 || port > 65535) {
			throw new IllegalArgumentException("Invalid start port: " + port);
		}

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
			logger.error("portAvailable {}", e.getMessage());
		} finally  {
			if (ds != null)  {
				ds.close();
			}

			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e)  {
					/* should not be thrown */
					logger.error("portAvailable : finally {}", e.getMessage());
				}
			}
		}
		return false;
	}
}