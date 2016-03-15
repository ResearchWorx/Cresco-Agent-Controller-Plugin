package ActiveMQ;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ServiceStopper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.RandomString;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;

public class ActiveBroker {
	public static BrokerService broker;
	public static TransportConnector connector;
	private static final Logger logger = LoggerFactory.getLogger(ActiveBroker.class);

	public ActiveBroker(String brokerName) {
		logger.debug("Broker initialized");
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

				connector = new TransportConnector();

				connector.setUri(new URI("tcp://[::]:32010"));
				broker.addConnector(connector);

				broker.start();
				//broker.getManagementContext().start();
				while(!broker.isStarted() /*|| !broker.getManagementContext().isConnectorStarted()*/) {
			    	Thread.sleep(1000);
			    }
			} else {
				logger.error("Constructor : portAvailable(32010) == false");
			}
		} catch(Exception ex) {
			logger.error("Init {}", ex.getMessage());
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
			RandomString rs = new RandomString(5);
			
			bridge.setName(rs.nextString());
			bridge.setDuplex(true);
			bridge.setDynamicOnly(true);
			bridge.setPrefetchSize(1);
		} catch(Exception ex) {
			logger.error("AddNetworkConnector {}", ex.getMessage());
		}
		return bridge;
	}
	
	public static void AddTransportConnector(String URI) {
		try {
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));
			broker.addConnector(connector);
			broker.startTransportConnector(connector);
		} catch(Exception ex) {
			logger.error("AddTransportConnector {}", ex.getMessage());
		}
	}
	
	public static boolean portAvailable(int port) {
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