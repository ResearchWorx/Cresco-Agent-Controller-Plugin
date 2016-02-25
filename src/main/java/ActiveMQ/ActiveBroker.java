package ActiveMQ;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.slf4j.LoggerFactory;

import shared.RandomString;
import ch.qos.logback.classic.Level;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;


public class ActiveBroker {

	public static BrokerService broker;
	
	private NetworkConnector bridge(BrokerService from, BrokerService to) throws Exception {
		   TransportConnector toConnector = to.getTransportConnectors().get(0);
		   NetworkConnector bridge = from.addNetworkConnector("static://" + toConnector.getPublishableConnectString());
		   //bridge.addStaticallyIncludedDestination(sendQ);
		   //bridge.addStaticallyIncludedDestination(replyQWildcard);
		   return bridge;
		}
	/*
	protected NetworkConnector bridgeBrokers(String localBrokerName,
            String remoteBrokerName,
            boolean dynamicOnly,
            int networkTTL) throws Exception {
NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName, dynamicOnly, networkTTL, true);
connector.setBridgeTempDestinations(true);
connector.setAdvisoryForFailedForward(true);
connector.setDuplex(useDuplex);
connector.setAlwaysSyncSend(true);
networkConnectors.add(connector);
return connector;
}
	*/
	public ActiveBroker(String brokerName)
	{
		//ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	//rootLogger.setLevel(Level.toLevel("debug"));
    	//rootLogger.setLevel(Level.OFF);
		/*
		<destinationPolicy>
	     <policyMap>
	        <policyEntries>
	           <policyEntry queue=">" gcInactiveDestinations="true" inactiveTimoutBeforeGC="30000"/>
	        </policyEntries>
	     </policyMap>
	  </destinationPolicy>
	      */ 
		
		try
		{
			if(portAvailable(1099))
			{
				broker = new BrokerService();
				broker.setPersistent(false);
				broker.setBrokerName(brokerName);
				broker.setSchedulePeriodForDestinationPurge(10000);
				PolicyMap pm = broker.getDestinationPolicy();
				PolicyEntry pe = pm.getDefaultEntry();
				pe.setGcInactiveDestinations(true);
				pe.setInactiveTimeoutBeforeGC(30000);
				pm.setDefaultEntry(pe);
				broker.setDestinationPolicy(pm);
			    //NetworkConnector connector = bridge
				//connector.
				// = new NetworkConnector();
				//TransportConnector connectorIPv4 = new TransportConnector();
				TransportConnector connector = new TransportConnector();
				
				//connectorIPv4.setUri(new URI("tcp://0.0.0.0:32010")); //all ipv4 addresses
				connector.setUri(new URI("tcp://[::]:32010"));
				//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
				//broker.addConnector(connectorIPv4);
				broker.addConnector(connector);
				
				//broker.addNetworkConnector(new URI("multicast://default?group=test"));
				//NetworkConnector bridge = broker.addNetworkConnector(new URI("static://" + remoteIP + ":32010"));
				//bridge.setUserName(userName);
				broker.start();
				broker.getManagementContext().start();
				while(!broker.isStarted() || !broker.getManagementContext().isConnectorStarted())
			    {
			    	Thread.sleep(1000);
			    }
			}
		}
		catch(Exception ex)
		{
			System.out.println("ActiveBroker Init : " + ex.toString());	
		}
		
	}	

	public boolean RemoteNetworkConnector(NetworkConnector bridge)
	{
		//boolean isRemoved = false;
		
		return broker.removeNetworkConnector(bridge);
		//return isRemoved;
		
	}
	public NetworkConnector AddNetworkConnector(String URI)
	{
		NetworkConnector bridge = null;
		try
		{
			bridge = broker.addNetworkConnector(new URI("static:tcp://" + URI + ":32010"));
			RandomString rs = new RandomString(5);
			
			bridge.setName(rs.nextString());
			bridge.setDuplex(false);
			//TransportConnector connector = new TransportConnector();
			//connector.setUri(new URI(URI));
			//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
			//broker.addConnector(connector);
			//broker.requestRestart();
			//broker.startAllConnectors();
			//broker.startTransportConnector(connector);
			
			//System.out.println("BorkerNAme: " + bridge.getBrokerName() + " " + bridge.getBrokerService().getBrokerName());
			//bridge.start();
			//System.out.println("BorkerNAme: " + bridge.getBrokerName() + " " + bridge.getBrokerService().getDefaultSocketURIString());
			
			
		}
		catch(Exception ex)
		{
			System.out.println("ActiveBroker : AddNetworkConnector Error : " + ex.toString());
		}
		return bridge;
	}
	
	public static void AddTransportConnector(String URI)
	{
		try
		{
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));
			//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
			broker.addConnector(connector);
			//broker.requestRestart();
			//broker.startAllConnectors();
			broker.startTransportConnector(connector);
		}
		catch(Exception ex)
		{
			
		}
	}
	
	
	public static boolean portAvailable(int port) 
	{
		if (port < 0 || port > 65535) 
		{
			throw new IllegalArgumentException("Invalid start port: " + port);
		}

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try 
		{
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} 
		catch (IOException e) 
		{
		} 
		finally 
		{
			if (ds != null) 
			{
				ds.close();
			}

			if (ss != null) 
			{
				try 
				{
					ss.close();
				} 
				catch (IOException e) 
				{
					/* should not be thrown */
				}
			}
		}

		return false;
		}

	}



